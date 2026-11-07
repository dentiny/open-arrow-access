//! PMC article metadata and original PDF URLs from a bucket or local mirror.
//!
//! The byte accessor must be rooted at `pmc-oa-opendata` (or a mirror preserving
//! its object paths). Metadata without a PDF URL produces no row. PDF contents
//! are never read. Discovery and metadata reads are sequential; listing order
//! and explicit duplicates survive.

mod accessor;
mod input;
mod metadata;
mod reader;
mod request;
mod validation;

pub use accessor::PmcAccessor;
pub use request::{PmcInput, PmcLimits, PmcRequest};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use open_arrow_access_core::{Capability, Error, ErrorKind};
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};

/// Fixed article-metadata schema with the original PDF URL and source JSON.
pub fn schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new_with_metadata(
                vec![
                    Field::new("pmcid", DataType::Utf8, false),
                    Field::new("pmc_version", DataType::UInt32, true),
                    Field::new("pmid", DataType::Utf8, true),
                    Field::new("doi", DataType::Utf8, true),
                    Field::new("title", DataType::LargeUtf8, true),
                    Field::new("license", DataType::Utf8, true),
                    Field::new("source_uri", DataType::Utf8, false),
                    Field::new("raw_metadata_json", DataType::LargeUtf8, true),
                ],
                HashMap::from([("pmc_arrow.row_kind".into(), "article_metadata".into())]),
            ))
        })
        .clone()
}

fn capability(read: bool) -> Capability {
    Capability {
        read,
        read_with_range: false,
        read_parallel: false,
        list: false,
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidInput, "pmc", message)
}

fn decode(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Decode, "read_pmc", message)
}
