//! One article-version JSON object per row; content URLs are not fetched.

mod accessor;
mod metadata;
mod validation;

pub use accessor::PmcAccessor;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use open_arrow_access_core::{Error, ErrorKind};
use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{Arc, OnceLock},
};

const DEFAULT_METADATA_BYTES: NonZeroUsize = NonZeroUsize::new(4 * 1024 * 1024).unwrap();

/// Maximum decoded metadata bytes.
#[derive(Clone, Copy, Debug)]
pub struct PmcLimits {
    pub metadata_bytes: NonZeroUsize,
}
impl Default for PmcLimits {
    fn default() -> Self {
        Self {
            metadata_bytes: DEFAULT_METADATA_BYTES,
        }
    }
}

/// Article metadata fields and verbatim source JSON.
pub fn schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new_with_metadata(
                vec![
                    Field::new("pmcid", DataType::Utf8, false),
                    Field::new("version", DataType::UInt32, true),
                    Field::new("pmid", DataType::Utf8, true),
                    Field::new("doi", DataType::Utf8, true),
                    Field::new("title", DataType::LargeUtf8, true),
                    Field::new("citation", DataType::LargeUtf8, true),
                    Field::new("is_pmc_openaccess", DataType::Boolean, true),
                    Field::new("is_manuscript", DataType::Boolean, true),
                    Field::new("is_historical_ocr", DataType::Boolean, true),
                    Field::new("is_retracted", DataType::Boolean, true),
                    Field::new("license_code", DataType::Utf8, true),
                    Field::new("xml_url", DataType::Utf8, true),
                    Field::new("pdf_url", DataType::Utf8, true),
                    Field::new(
                        "media_urls",
                        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                        true,
                    ),
                    Field::new("text_url", DataType::Utf8, true),
                    Field::new("raw_metadata_json", DataType::LargeUtf8, false),
                ],
                HashMap::from([("pmc.row_kind".into(), "article_metadata".into())]),
            ))
        })
        .clone()
}
fn decode(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Decode, "read_pmc", message)
}
