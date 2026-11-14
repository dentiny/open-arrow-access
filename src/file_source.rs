use std::sync::{Arc, Mutex};

use crate::{
    Accessor, AccessorExt, ByteAccessor, FactoryAccessor, ReadRequest, ReaderFactory,
    RecordMetadata, RecordReader, Result, SchemaRef,
    decompression::AutoDecompressionLayer,
    storage::{OpenDalAccessor, opendal},
};

mod error;

/// Compression surrounding a complete file, independent of format-internal codecs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OuterCompression {
    /// Decode the original bytes, preserving storage range access. Use for Parquet.
    None,
    /// Select an enabled streaming decoder from the final object-key suffix.
    /// Unknown suffixes pass through; disabled codecs return Unsupported.
    /// ZIP and TAR member selection remains explicit.
    Auto,
}

/// Open a typed file source with metadata available and independently owned readers.
///
/// Configure the OpenDAL operator (backend, endpoint, credentials and policies)
/// in the application. `request.object` is its object key, not a URI resolver.
/// Format preparation precedes storage I/O. This function binds storage, optional
/// decompression, format and the complete request without creating a runtime.
/// This utility is always available. Enable the chosen format and automatic
/// decompression codecs through their existing features.
///
/// ```no_run
/// # #[cfg(feature = "format-json")]
/// # async fn example(operator: open_arrow_access::storage::opendal::Operator)
/// # -> open_arrow_access::Result<()> {
/// use open_arrow_access::{open_file_source, OuterCompression, ReadRequest};
/// use open_arrow_access::file_format::json::JsonFormat;
///
/// let source = open_file_source(
///     operator,
///     ReadRequest::new("events.jsonl"),
///     JsonFormat::infer(),
///     OuterCompression::Auto,
/// ).await?;
/// println!("{:?}", source.schema());
/// let mut reader = source.reader().await?;
/// while let Some(batch) = reader.next().await? { /* consume batch */ }
/// # Ok(())
/// # }
/// ```
pub async fn open_file_source<F>(
    operator: opendal::Operator,
    request: ReadRequest,
    format: F,
    compression: OuterCompression,
) -> Result<FileSource>
where
    F: ReaderFactory<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
{
    let storage = OpenDalAccessor::new(operator);
    let storage: Arc<dyn ByteAccessor> = match compression {
        OuterCompression::None => Arc::new(storage),
        OuterCompression::Auto => Arc::new(storage.layer(AutoDecompressionLayer::new())),
    };
    FileSource::bind(FactoryAccessor::new(storage, format), request).await
}

/// A file selection bound to its initial metadata and read options.
///
/// The reader opened during binding is retained after schema inspection, avoiding
/// another open or inference pass on first consumption. Later calls reopen independently and
/// require the same output schema, including metadata. Reader cursors are never
/// shared; readers remain usable after dropping the source. Binding fixes the
/// schema, not the file contents, and adds no snapshot guarantee.
pub struct FileSource {
    accessor: Box<dyn Accessor<Request = ReadRequest, Reader = dyn RecordReader>>,
    request: ReadRequest,
    metadata: RecordMetadata,
    bound_reader: Mutex<Option<Box<dyn RecordReader>>>,
}

impl FileSource {
    async fn bind(
        accessor: impl Accessor<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
        request: ReadRequest,
    ) -> Result<Self> {
        let reader = accessor.open(request.clone()).await?;
        let metadata = reader.metadata().clone();
        Ok(Self {
            accessor: Box::new(accessor),
            request,
            metadata,
            bound_reader: Mutex::new(Some(reader)),
        })
    }

    /// Metadata established by the initial open, available before consuming rows.
    pub fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }

    /// Share the fixed output schema without opening or polling another reader.
    pub fn schema(&self) -> SchemaRef {
        self.metadata.schema.clone()
    }

    /// Take the bound reader once, then reopen with the original request.
    /// Schema drift returns Decode before yielding a reader; source errors pass through.
    pub async fn reader(&self) -> Result<Box<dyn RecordReader>> {
        let bound = self
            .bound_reader
            .lock()
            .map_err(|_| error::source_state_unavailable())?
            .take();
        let reader = match bound {
            Some(reader) => reader,
            None => self.accessor.open(self.request.clone()).await?,
        };
        if reader.metadata().schema != self.metadata.schema {
            return Err(error::schema_changed());
        }
        Ok(reader)
    }
}
