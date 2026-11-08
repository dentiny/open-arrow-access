//! One article-version JSON object per row; content URLs are not fetched.

mod accessor;
mod error;
mod metadata;
mod validation;

pub use accessor::PmcAccessor;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};

/// Article metadata fields.
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
                ],
                HashMap::from([("pmc.row_kind".into(), "article_metadata".into())]),
            ))
        })
        .clone()
}
