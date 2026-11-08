//! One book per row, with original RDF and selected files.

mod metadata;

use std::{
    collections::HashSet,
    num::NonZeroUsize,
    sync::{Arc, OnceLock},
};

use arrow_schema::{DataType, Field, Schema};
use open_arrow_access_core::{
    Accessor, ByteReadOptions, ByteReadRequest, ByteReader, Capability, DocumentAccessor,
    DocumentDecoder, Error, ErrorKind, ObjectRef, ReadOptions, ReadOptionsProvider, ReadRequest,
    RecordBatch, RecordReader, Result, SchemaRef, async_trait, read_object,
};
use xml_arrow::{NamespaceBinding, XmlField, XmlSchema};

use metadata::BookMetadata;

/// Source byte limits; excludes parser and Arrow allocations.
#[derive(Clone, Copy, Debug)]
pub struct GutenbergLimits {
    /// RDF byte limit (default: 4 MiB).
    pub metadata_bytes: NonZeroUsize,
    /// Per-file byte limit (default: 256 MiB).
    pub file_bytes: NonZeroUsize,
    /// Combined RDF and file byte limit (default: 1 GiB).
    pub row_bytes: NonZeroUsize,
}

impl Default for GutenbergLimits {
    fn default() -> Self {
        Self {
            metadata_bytes: NonZeroUsize::new(4 * 1024 * 1024).unwrap(),
            file_bytes: NonZeroUsize::new(256 * 1024 * 1024).unwrap(),
            row_bytes: NonZeroUsize::new(1024 * 1024 * 1024).unwrap(),
        }
    }
}

/// Map an RDF file URI to a storage locator.
#[derive(Clone, Debug)]
pub struct GutenbergFile {
    pub uri: String,
    pub object: ObjectRef,
}

impl GutenbergFile {
    pub fn new(uri: impl Into<String>, object: impl Into<ObjectRef>) -> Self {
        Self {
            uri: uri.into(),
            object: object.into(),
        }
    }
}

/// Book RDF and files in output order. An empty selection embeds no files.
#[derive(Clone, Debug)]
pub struct GutenbergRequest {
    pub metadata: ReadRequest,
    pub files: Vec<GutenbergFile>,
}

impl GutenbergRequest {
    pub fn new(metadata: impl Into<ObjectRef>, files: Vec<GutenbergFile>) -> Self {
        Self {
            metadata: ReadRequest::new(metadata),
            files,
        }
    }

    pub fn with_options(mut self, options: ReadOptions) -> Self {
        self.metadata.options = options;
        self
    }

    /// Apply I/O settings to RDF and file reads.
    pub fn with_byte_options(mut self, options: ByteReadOptions) -> Self {
        self.metadata.byte_options = options;
        self
    }
}

impl ReadOptionsProvider for GutenbergRequest {
    fn read_options(&self) -> &ReadOptions {
        &self.metadata.options
    }

    fn read_options_mut(&mut self) -> &mut ReadOptions {
        &mut self.metadata.options
    }
}

impl From<&GutenbergRequest> for ByteReadRequest {
    fn from(request: &GutenbergRequest) -> Self {
        Self::from(&request.metadata)
    }
}

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
        let book = BookMetadata::parse(raw).await?;
        // Validate membership even when projection omits file data.
        let selections = request
            .files
            .iter()
            .map(|selection| {
                book.file_index(&selection.uri)
                    .map(|index| (selection, index))
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
                let data = read_object(
                    self.inner.as_ref(),
                    ByteReadRequest::new(selection.object.clone())
                        .with_options(request.metadata.byte_options),
                    self.limits.file_bytes.get().min(remaining),
                    "read_gutenberg",
                )
                .await?;
                remaining -= data.len();
                files.push((file, data));
            }
        }
        book.into_batch(&files, schema)
    }
}

// Declare Arrow fields and their XML locations together. The XML engine extracts
// scalars as strings; BookMetadata::parse converts the types it cannot emit directly.
type Mapping = (Field, XmlField);

fn scalar(name: &str, path: &str, ty: DataType, nullable: bool) -> Mapping {
    (
        Field::new(name, ty, nullable),
        XmlField::new(name, path, DataType::Utf8, nullable),
    )
}

fn strings(name: &str, path: &str) -> Mapping {
    let xml = XmlField::list(
        name,
        ".",
        false,
        XmlField::new("item", path, DataType::Utf8, false),
    );
    (xml.arrow_field().clone(), xml)
}

fn objects(name: &str, path: &str, container: &str, mappings: Vec<Mapping>) -> Mapping {
    let (mut fields, xml): (Vec<_>, Vec<_>) = mappings.into_iter().unzip();
    let xml = XmlField::list(
        name,
        ".",
        false,
        XmlField::structure(
            "item",
            path,
            false,
            vec![XmlField::structure("value", container, false, xml)],
        ),
    );
    if name == "files" {
        fields.push(Field::new("data", DataType::LargeBinary, false));
    }
    let item = Arc::new(Field::new("item", DataType::Struct(fields.into()), false));
    let ty = if name == "files" {
        DataType::LargeList(item)
    } else {
        DataType::List(item)
    };
    (Field::new(name, ty, false), xml)
}

fn definitions() -> &'static (SchemaRef, XmlSchema) {
    static DEFINITIONS: OnceLock<(SchemaRef, XmlSchema)> = OnceLock::new();
    DEFINITIONS.get_or_init(|| {
        use DataType::*;
        let mappings = vec![
            scalar("ebook_id", "@r:about", UInt64, false),
            scalar("title", "d:title", LargeUtf8, true),
            objects(
                "creator",
                "d:creator",
                "p:agent",
                vec![
                    scalar("uri", "@r:about", Utf8, true),
                    scalar("name", "p:name", Utf8, true),
                    strings("alias", "p:alias"),
                    scalar("birthdate", "p:birthdate", Int32, true),
                    scalar("deathdate", "p:deathdate", Int32, true),
                ],
            ),
            strings("language", "d:language/r:Description/r:value"),
            objects(
                "subject",
                "d:subject",
                "r:Description",
                vec![
                    scalar("scheme", "a:memberOf/@r:resource", Utf8, true),
                    scalar("value", "r:value", Utf8, false),
                ],
            ),
            strings("bookshelf", "p:bookshelf/r:Description/r:value"),
            scalar("issued", "d:issued", Date32, true),
            scalar("rights", "d:rights", LargeUtf8, true),
            objects(
                "files",
                "d:hasFormat",
                "p:file",
                vec![
                    scalar("uri", "@r:about", Utf8, false),
                    strings("format", "d:format/r:Description/r:value"),
                    scalar("extent", "d:extent", UInt64, true),
                    scalar("modified", "d:modified", Utf8, true),
                ],
            ),
        ];
        let (mut fields, xml): (Vec<_>, Vec<_>) = mappings.into_iter().unzip();
        fields.insert(
            fields.len() - 1,
            Field::new("metadata_rdf", LargeBinary, false),
        );
        let bindings = [
            ("r", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
            ("p", "http://www.gutenberg.org/2009/pgterms/"),
            ("d", "http://purl.org/dc/terms/"),
            ("a", "http://purl.org/dc/dcam/"),
        ]
        .map(|(prefix, uri)| NamespaceBinding { prefix, uri });
        let xml = XmlSchema::with_namespaces(
            vec![XmlField::structure("book", "p:ebook", false, xml)],
            &bindings,
        )
        .expect("valid Gutenberg XML mapping");
        (Arc::new(Schema::new(fields)), xml)
    })
}

/// Book schema; `metadata_rdf` retains unmapped fields.
pub fn schema() -> SchemaRef {
    definitions().0.clone()
}

fn mapping() -> XmlSchema {
    definitions().1.clone()
}

fn decode(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Decode, "read_gutenberg", message)
}

fn decode_source(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    decode(error.to_string()).with_source(error)
}
