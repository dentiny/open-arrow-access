use std::sync::Mutex;

use crate::{
    Accessor, Error, ErrorKind, RecordMetadata, RecordReader, Result, SchemaRef, async_trait,
};

/// A typed record selection bound to its initial metadata and schema.
///
/// Binding opens one reader and retains it without polling any rows. The first
/// `reader()` takes that reader; later calls open independent readers using a
/// clone of the original request. The complete Arrow schema, including metadata,
/// must match the bound schema before a reopened reader is returned. Binding
/// fixes the schema, not the contents or upstream version of the selection.
///
/// Accessor and request types are erased after binding, allowing different record
/// providers to share one source type. No runtime is created and no producer tasks
/// are spawned. Returned readers own their state and outlive this source.
pub struct BoundSource {
    selection: Box<dyn BoundSelection>,
    metadata: RecordMetadata,
    reader: Mutex<Option<Box<dyn RecordReader>>>,
}

/// A saved accessor and typed request behind a common record-reader interface.
#[async_trait]
trait BoundSelection: Send + Sync {
    /// Open an independent reader with a clone of the stored typed request.
    /// Accessor errors propagate unchanged; the caller validates the reader's schema.
    async fn open(&self) -> Result<Box<dyn RecordReader>>;
}

struct Selection<A: Accessor> {
    accessor: A,
    request: A::Request,
}

#[async_trait]
impl<A> BoundSelection for Selection<A>
where
    A: Accessor<Reader = dyn RecordReader>,
    A::Request: Clone + Sync,
{
    async fn open(&self) -> Result<Box<dyn RecordReader>> {
        self.accessor.open(self.request.clone()).await
    }
}

impl BoundSource {
    /// Bind any record accessor and its complete typed request.
    ///
    /// The request must be cloneable and `Send + Sync + 'static`; it does not need
    /// to be a `ReadRequest` or implement `ReadOptionsProvider`. The accessor is
    /// owned and need not implement `Clone`. An `Arc` of a concrete/dynamic accessor
    /// can also be passed. Open errors propagate unchanged.
    ///
    /// ```no_run
    /// use open_arrow_access_core::{Accessor, BoundSource, RecordReader, Result};
    ///
    /// async fn consume<A>(accessor: A, request: A::Request) -> Result<()>
    /// where
    ///     A: Accessor<Reader = dyn RecordReader> + 'static,
    ///     A::Request: Clone + Sync + 'static,
    /// {
    ///     let source = BoundSource::bind(accessor, request).await?;
    ///     println!("{:?}", source.schema());
    ///     let mut reader = source.reader().await?;
    ///     while let Some(batch) = reader.next().await? {
    ///         println!("{} rows", batch.num_rows());
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub async fn bind<A>(accessor: A, request: A::Request) -> Result<Self>
    where
        A: Accessor<Reader = dyn RecordReader> + 'static,
        A::Request: Clone + Sync + 'static,
    {
        let reader = accessor.open(request.clone()).await?;
        let metadata = reader.metadata().clone();
        Ok(Self {
            selection: Box::new(Selection { accessor, request }),
            metadata,
            reader: Mutex::new(Some(reader)),
        })
    }

    /// Metadata established by binding, available without polling the reader.
    pub fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }

    /// The bound output schema, including its metadata.
    pub fn schema(&self) -> SchemaRef {
        self.metadata.schema.clone()
    }

    /// Take the initial reader once, then reopen independently with the bound request.
    ///
    /// The state lock is released before an async open; concurrent calls never
    /// share a reader cursor. Schema drift returns Decode before yielding the new
    /// reader, while accessor and subsequent stream errors pass through unchanged.
    pub async fn reader(&self) -> Result<Box<dyn RecordReader>> {
        let initial = self
            .reader
            .lock()
            .map_err(|_| {
                Error::new(
                    ErrorKind::Io,
                    "bound_source_reader",
                    "bound source state is poisoned",
                )
            })?
            .take();
        let reader = match initial {
            Some(reader) => reader,
            None => self.selection.open().await?,
        };
        crate::validate_schema(&reader.metadata().schema, &self.metadata.schema, || {
            Error::new(
                ErrorKind::Decode,
                "bound_source_reader",
                "source schema differs from the schema fixed at bind",
            )
        })?;
        Ok(reader)
    }
}
