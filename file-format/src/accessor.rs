use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, ReadRequest, RecordReader, Result,
    async_trait,
};

use crate::FileFormat;

/// Compose byte storage with a [`FileFormat`] using the common read protocol.
///
/// Owns storage and format configuration. Each open prepares independent format
/// state, forwards the object and byte options unchanged, and transfers ownership
/// of the byte reader to the format. Output exposes record reading only.
/// Built-in accessor names are aliases with their existing constructors;
/// [`Self::from_format`] also accepts application-defined formats.
///
/// ```no_run
/// # #[cfg(feature = "json")]
/// # async fn example<B>(bytes: B, schema: open_arrow_access_core::SchemaRef)
/// # -> open_arrow_access_core::Result<()>
/// # where B: open_arrow_access_core::Accessor<
/// # Request = open_arrow_access_core::ByteReadRequest,
/// # Reader = dyn open_arrow_access_core::ByteReader> {
/// use open_arrow_access_core::{Accessor, ReadRequest};
/// use open_arrow_access_file_format::{FileAccessor, JsonFormat};
///
/// let files = FileAccessor::from_format(bytes, JsonFormat::new(schema));
/// let mut reader = files.open(ReadRequest::new("records.jsonl")).await?;
/// while let Some(batch) = reader.next().await? { /* consume batch */ }
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct FileAccessor<B, F> {
    inner: B,
    pub(crate) format: F,
}

impl<B, F> FileAccessor<B, F> {
    /// Bind storage and format configuration without opening input.
    pub fn from_format(inner: B, format: F) -> Self {
        Self { inner, format }
    }
}

#[async_trait]
impl<B, F> Accessor for FileAccessor<B, F>
where
    B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
    F: FileFormat,
{
    type Request = ReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> Capability {
        Capability {
            read: self.format.can_read(self.inner.capability()),
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        let prepared = self.format.prepare(&request.options)?;
        let input = self
            .inner
            .open(ByteReadRequest::new(request.object.clone()).with_options(request.byte_options))
            .await?;
        self.format.open(input, request, prepared).await
    }
}
