use std::sync::Arc;

use open_arrow_access_core::{
    Accessor, Capability, Error, ErrorKind, ReadRequest, RecordMetadata, RecordReader, Result,
    async_trait,
};

use super::{FileSetRequest, reader::FileSetReader};

/// Read files sequentially through one supplied format accessor.
///
/// The first file determines the fixed output schema. Later files must have
/// exactly the same projected schema, including metadata. An empty selection
/// is invalid; an empty file with a known schema is supported.
#[derive(Debug)]
pub struct FileSetAccessor<A> {
    inner: Arc<A>,
}

impl<A> Clone for FileSetAccessor<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<A> FileSetAccessor<A> {
    pub fn new(inner: A) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

#[async_trait]
impl<A> Accessor for FileSetAccessor<A>
where
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
{
    type Request = FileSetRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> Capability {
        Capability {
            read: self.inner.capability().read,
            ..Capability::default()
        }
    }

    async fn open(&self, request: FileSetRequest) -> Result<Box<dyn RecordReader>> {
        let mut objects = request.objects.into_iter();
        let first = objects.next().ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                "open_file_set",
                "file selection is empty",
            )
        })?;
        let reader = self
            .inner
            .open(
                ReadRequest::new(first)
                    .with_options(request.options.clone())
                    .with_byte_options(request.byte_options),
            )
            .await?;
        let metadata = RecordMetadata {
            schema: reader.metadata().schema.clone(),
            source: None,
        };
        Ok(Box::new(FileSetReader::new(
            self.inner.clone(),
            reader,
            objects,
            request.options,
            request.byte_options,
            metadata,
        )))
    }
}
