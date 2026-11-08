use std::{collections::HashSet, sync::Arc};

use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, DocumentAccessor, DocumentDecoder, Error,
    ErrorKind, RecordBatch, RecordReader, Result, SchemaRef, async_trait, read_limited,
};

use super::{GutenbergLimits, GutenbergRequest, batch::create_batch, decode, metadata, schema};

/// Read per-book RDF and selected files through caller-configured storage.
pub struct GutenbergAccessor<A> {
    inner: Arc<A>,
    limits: GutenbergLimits,
}

impl<A> GutenbergAccessor<A> {
    pub fn new(inner: A) -> Self {
        Self {
            inner: Arc::new(inner),
            limits: GutenbergLimits::default(),
        }
    }
    pub fn with_limits(mut self, limits: GutenbergLimits) -> Self {
        self.limits = limits;
        self
    }
}

#[async_trait]
impl<A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static> Accessor
    for GutenbergAccessor<A>
{
    type Request = GutenbergRequest;
    type Reader = dyn RecordReader;
    fn capability(&self) -> Capability {
        Capability {
            read: self.inner.capability().read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }
    async fn open(&self, request: GutenbergRequest) -> Result<Box<dyn RecordReader>> {
        DocumentAccessor::new(
            self.inner.clone(),
            GutenbergDecoder {
                inner: self.inner.clone(),
                limits: self.limits,
            },
            self.limits
                .metadata_bytes
                .get()
                .min(self.limits.row_bytes.get()),
        )
        .open(request)
        .await
    }
}

struct GutenbergDecoder<A> {
    inner: Arc<A>,
    limits: GutenbergLimits,
}

#[async_trait]
impl<A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static> DocumentDecoder
    for GutenbergDecoder<A>
{
    type Request = GutenbergRequest;

    fn prepare(&self, request: &GutenbergRequest) -> Result<SchemaRef> {
        let mut seen = HashSet::new();
        for file in &request.files {
            if file.uri.is_empty() || !seen.insert(&file.uri) {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "open_gutenberg",
                    "file URIs must be nonempty and unique",
                ));
            }
        }
        Ok(schema())
    }

    async fn decode(
        &self,
        raw: &[u8],
        request: GutenbergRequest,
        schema: SchemaRef,
    ) -> Result<RecordBatch> {
        let book = metadata::parse(raw)?;
        // Validate membership even when projection omits file data.
        let selections = request
            .files
            .iter()
            .map(|selection| {
                book.files
                    .iter()
                    .find(|file| file.uri == selection.uri)
                    .map(|file| (selection, file))
                    .ok_or_else(|| {
                        decode(format!(
                            "selected file is absent from RDF: {}",
                            selection.uri
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut files = Vec::new();
        if schema.index_of("files").is_ok() {
            let mut remaining = self.limits.row_bytes.get() - raw.len();
            for (selection, file) in selections {
                let input = self
                    .inner
                    .open(
                        ByteReadRequest::new(selection.object.clone())
                            .with_options(request.metadata.byte_options),
                    )
                    .await?;
                let data = read_limited(
                    input,
                    self.limits.file_bytes.get().min(remaining),
                    "read_gutenberg",
                )
                .await?;
                remaining -= data.len();
                files.push((file, data));
            }
        }
        create_batch(&book, raw, &files, schema)
    }
}
