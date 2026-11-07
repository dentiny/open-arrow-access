use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, ReaderFactory, ReaderFactoryExt, Result,
    async_trait,
};

/// Compose byte storage with a [`ReaderFactory`] using the common read protocol.
///
/// Owns storage and format configuration. Each open prepares independent format
/// state, forwards the object and byte options unchanged, and transfers ownership
/// of the byte reader to the factory. Request and output types come from the factory.
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
    F: ReaderFactory,
    for<'a> ByteReadRequest: From<&'a F::Request>,
{
    type Request = F::Request;
    type Reader = F::Reader;

    fn capability(&self) -> Capability {
        self.format.capability(self.inner.capability())
    }

    async fn open(&self, request: Self::Request) -> Result<Box<Self::Reader>> {
        ReaderFactoryExt::open_reader(&self.format, &self.inner, request).await
    }
}
