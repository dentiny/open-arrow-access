//! One book per row, using the fields mapped from RDF.

mod metadata;

use std::sync::{Arc, OnceLock};

use arrow_schema::{DataType, Field, Schema};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, DocumentAccessor, DocumentDecoder, Error,
    ErrorKind, FieldValue, ReadRequest, RecordBatch, RecordReader, Result, SchemaRef, async_trait,
};
use xml_arrow::{NamespaceBinding, XmlField, XmlSchema};

/// Read per-book RDF metadata through caller-configured storage.
pub struct GutenbergAccessor<A> {
    inner: Arc<A>,
}

impl<A> GutenbergAccessor<A> {
    pub fn new(inner: A) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

#[async_trait]
impl<A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static> Accessor
    for GutenbergAccessor<A>
{
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    fn capability(&self) -> Capability {
        Capability {
            read: self.inner.capability().read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }
    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        DocumentAccessor::new(self.inner.clone(), GutenbergDecoder)
            .open(request)
            .await
    }
}

struct GutenbergDecoder;

#[async_trait]
impl DocumentDecoder for GutenbergDecoder {
    type Request = ReadRequest;

    fn prepare(&self, _request: &ReadRequest) -> Result<SchemaRef> {
        Ok(schema())
    }

    async fn decode(
        &self,
        raw: &[u8],
        _request: ReadRequest,
        schema: SchemaRef,
    ) -> Result<RecordBatch> {
        metadata::read_metadata(raw, schema).await
    }
}

// Declare Arrow fields and their XML locations together. The XML engine extracts
// scalars as strings; metadata conversion validates them and writes the output
// types when building the final batch.
type ScalarParser = for<'a> fn(&'a str) -> Result<FieldValue<'a>>;

struct Mapping {
    field: Field,
    xml: XmlField,
    children: Vec<Mapping>,
    parser: Option<ScalarParser>,
}

impl Mapping {
    fn new(field: Field, xml: XmlField) -> Self {
        Self {
            field,
            xml,
            children: Vec::new(),
            parser: None,
        }
    }

    fn parse_with(mut self, parser: ScalarParser) -> Self {
        self.parser = Some(parser);
        self
    }
}

fn scalar(name: &str, path: &str, ty: DataType, nullable: bool) -> Mapping {
    Mapping::new(
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
    Mapping::new(xml.arrow_field().clone(), xml)
}

fn objects(name: &str, path: &str, container: &str, mappings: Vec<Mapping>) -> Mapping {
    let fields: Vec<_> = mappings
        .iter()
        .map(|mapping| mapping.field.clone())
        .collect();
    let children = mappings.iter().map(|mapping| mapping.xml.clone()).collect();
    let xml = XmlField::list(
        name,
        ".",
        false,
        XmlField::structure(
            "item",
            path,
            false,
            vec![XmlField::structure("value", container, false, children)],
        ),
    );
    let item = Arc::new(Field::new("item", DataType::Struct(fields.into()), false));
    Mapping {
        children: mappings,
        ..Mapping::new(Field::new(name, DataType::List(item), false), xml)
    }
}

struct Definitions {
    schema: SchemaRef,
    xml: XmlSchema,
    fields: Vec<Mapping>,
}

fn definitions() -> &'static Definitions {
    static DEFINITIONS: OnceLock<Definitions> = OnceLock::new();
    DEFINITIONS.get_or_init(|| {
        use DataType::*;
        let mappings = vec![
            scalar("ebook_id", "@r:about", UInt64, false).parse_with(metadata::parse_ebook_id),
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
        let fields: Vec<_> = mappings
            .iter()
            .map(|mapping| mapping.field.clone())
            .collect();
        let children = mappings.iter().map(|mapping| mapping.xml.clone()).collect();
        let bindings = [
            ("r", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
            ("p", "http://www.gutenberg.org/2009/pgterms/"),
            ("d", "http://purl.org/dc/terms/"),
            ("a", "http://purl.org/dc/dcam/"),
        ]
        .map(|(prefix, uri)| NamespaceBinding { prefix, uri });
        let xml = XmlSchema::with_namespaces(
            vec![XmlField::structure("book", "p:ebook", false, children)],
            &bindings,
        )
        .expect("valid Gutenberg XML mapping");
        Definitions {
            schema: Arc::new(Schema::new(fields)),
            xml,
            fields: mappings,
        }
    })
}

/// Arrow schema for the mapped RDF fields.
pub fn schema() -> SchemaRef {
    definitions().schema.clone()
}

fn mapping() -> XmlSchema {
    definitions().xml.clone()
}

fn decode(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Decode, "read_gutenberg", message)
}

fn decode_source(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    decode(error.to_string()).with_source(error)
}
