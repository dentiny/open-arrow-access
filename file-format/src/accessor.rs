use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, FactoryAccessor, ReaderFactory, Result, async_trait,
};

/// Compose byte storage with a [`ReaderFactory`] using the common read protocol.
///
/// Each open prepares independent format state and forwards the object and byte
/// options unchanged. [`Self::from_format`] also accepts application-defined formats.
///
/// ```no_run
/// # #[cfg(feature = "json")]
/// # async fn example<B>(bytes: B, schema: open_arrow_access_core::SchemaRef)
/// # -> open_arrow_access_core::Result<()>
/// # where B: open_arrow_access_core::Accessor<
/// # Request = open_arrow_access_core::ByteReadRequest,
/// # Reader = dyn open_arrow_access_core::ByteReader> {
/// use open_arrow_access_core::{Accessor, ReadRequest};
/// use open_arrow_access_file_format::{FileAccessor, json::JsonFormat};
///
/// let files = FileAccessor::from_format(bytes, JsonFormat::new(schema));
/// let mut reader = files.open(ReadRequest::new("records.jsonl")).await?;
/// while let Some(batch) = reader.next().await? { /* consume batch */ }
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct FileAccessor<B, F> {
    pub(crate) binding: FactoryAccessor<B, F>,
    pub(crate) partition_options: crate::DecodePartitionOptions,
}

impl<B, F> FileAccessor<B, F> {
    /// Bind storage and format configuration without opening input.
    pub fn from_format(inner: B, format: F) -> Self {
        Self {
            binding: FactoryAccessor::new(inner, format),
            partition_options: crate::DecodePartitionOptions::default(),
        }
    }
    /// Configure bounded input units for supported file-internal decode planners.
    /// This does not alter the ordinary sequential reader.
    pub fn with_partition_options(mut self, options: crate::DecodePartitionOptions) -> Self {
        self.partition_options = options;
        self
    }

    pub fn map_format(self, map: impl FnOnce(F) -> F) -> Self {
        Self {
            binding: self.binding.map_factory(map),
            partition_options: self.partition_options,
        }
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

    fn capability(&self) -> <Self::Reader as open_arrow_access_core::Reader>::Capability {
        self.binding.capability()
    }

    async fn open(&self, request: Self::Request) -> Result<Box<Self::Reader>> {
        self.binding.open(request).await
    }
}
