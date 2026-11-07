//! One Gutenberg eBook per row, with original RDF and caller-selected file bytes.

mod accessor;
mod batch;
mod metadata;
mod request;

pub use accessor::GutenbergAccessor;
pub use request::{GutenbergFile, GutenbergRequest};

use std::{
    num::NonZeroUsize,
    sync::{Arc, OnceLock},
};

use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use open_arrow_access_core::{Error, ErrorKind};

/// Limits on collected source bytes, not process memory or Arrow allocations.
#[derive(Clone, Copy, Debug)]
pub struct GutenbergLimits {
    /// Maximum size of one per-book RDF document; defaults to 4 MiB.
    pub metadata_bytes: NonZeroUsize,
    /// Maximum size of one selected file; defaults to 256 MiB.
    pub file_bytes: NonZeroUsize,
    /// Maximum combined RDF and selected file bytes; defaults to 1 GiB.
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

fn list(name: &str, data_type: DataType) -> Field {
    Field::new(
        name,
        DataType::List(Arc::new(Field::new("item", data_type, false))),
        false,
    )
}

fn creator_fields() -> Fields {
    vec![
        Field::new("uri", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        list("alias", DataType::Utf8),
        Field::new("birthdate", DataType::Int32, true),
        Field::new("deathdate", DataType::Int32, true),
    ]
    .into()
}

fn subject_fields() -> Fields {
    vec![
        Field::new("scheme", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, false),
    ]
    .into()
}

fn file_fields() -> Fields {
    vec![
        Field::new("uri", DataType::Utf8, false),
        list("format", DataType::Utf8),
        Field::new("extent", DataType::UInt64, true),
        Field::new("modified", DataType::Utf8, true),
        Field::new("data", DataType::LargeBinary, false),
    ]
    .into()
}

/// Fixed schema. Unexpanded upstream fields remain in the verbatim `metadata_rdf`.
pub fn schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("ebook_id", DataType::UInt64, false),
                Field::new("title", DataType::LargeUtf8, true),
                list("creator", DataType::Struct(creator_fields())),
                list("language", DataType::Utf8),
                list("subject", DataType::Struct(subject_fields())),
                list("bookshelf", DataType::Utf8),
                Field::new("issued", DataType::Date32, true),
                Field::new("rights", DataType::LargeUtf8, true),
                Field::new("metadata_rdf", DataType::LargeBinary, false),
                Field::new(
                    "files",
                    DataType::LargeList(Arc::new(Field::new(
                        "item",
                        DataType::Struct(file_fields()),
                        false,
                    ))),
                    false,
                ),
            ]))
        })
        .clone()
}

fn decode(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Decode, "read_gutenberg", message)
}
