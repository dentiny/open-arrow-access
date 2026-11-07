use std::{collections::HashSet, sync::Arc};

use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, Error, ErrorKind, FactoryToken,
    ReaderFactory, ReaderFactoryExt, RecordMetadata, RecordReader, Result, SchemaRef, StreamReader,
    async_trait, read_limited,
};

use super::{GutenbergLimits, GutenbergRequest, batch::create_batch, decode, metadata, schema};

/// Read a per-book RDF and explicitly mapped files through caller-configured storage.
pub struct GutenbergAccessor<A> {
    factory: GutenbergFactory<A>,
}

impl<A> GutenbergAccessor<A> {
    pub fn new(inner: A) -> Self {
        Self {
            factory: GutenbergFactory {
                inner: Arc::new(inner),
                limits: GutenbergLimits::default(),
            },
        }
    }

    pub fn with_limits(mut self, limits: GutenbergLimits) -> Self {
        self.factory.limits = limits;
        self
    }
}

#[async_trait]
impl<A> Accessor for GutenbergAccessor<A>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static,
{
    type Request = GutenbergRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> Capability {
        self.factory.capability(self.factory.inner.capability())
    }

    async fn open(&self, request: GutenbergRequest) -> Result<Box<dyn RecordReader>> {
        self.factory.open_reader(&self.factory.inner, request).await
    }
}

struct GutenbergFactory<A> {
    inner: Arc<A>,
    limits: GutenbergLimits,
}

#[async_trait]
impl<A> ReaderFactory for GutenbergFactory<A>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static,
{
    type Request = GutenbergRequest;
    type Prepared = SchemaRef;
    type Reader = dyn RecordReader;

    fn prepare(&self, request: &GutenbergRequest) -> Result<SchemaRef> {
        let mut uris = HashSet::new();
        for file in &request.files {
            if file.uri.is_empty() || !uris.insert(&file.uri) {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "open_gutenberg",
                    "file URIs must be nonempty and unique",
                ));
            }
        }
        request
            .metadata
            .options
            .resolve_projection(&schema())
            .map(|(schema, _)| schema)
    }

    async fn create_reader(
        &self,
        _token: &FactoryToken,
        input: Box<dyn ByteReader>,
        request: GutenbergRequest,
        schema: SchemaRef,
    ) -> Result<Box<dyn RecordReader>> {
        let metadata = RecordMetadata {
            schema: schema.clone(),
            source: Some(request.metadata.object),
        };
        let inner = self.inner.clone();
        let limits = self.limits;
        let stream = stream::once(async move {
            let raw = read_limited(
                input,
                limits.metadata_bytes.get().min(limits.row_bytes.get()),
                "read_gutenberg",
            )
            .await?;
            let book = metadata::parse(&raw)?;
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
                let mut remaining = limits.row_bytes.get() - raw.len();
                for (selection, file) in selections {
                    let input = inner
                        .open(
                            ByteReadRequest::new(selection.object.clone())
                                .with_options(request.metadata.byte_options),
                        )
                        .await?;
                    let data = read_limited(
                        input,
                        limits.file_bytes.get().min(remaining),
                        "read_gutenberg",
                    )
                    .await?;
                    remaining -= data.len();
                    files.push((file, data));
                }
            }
            create_batch(&book, &raw, &files, schema)
        });
        Ok(Box::new(StreamReader::new(stream, metadata)))
    }
}
