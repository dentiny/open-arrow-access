//! PMC article metadata and original PDF snapshots from a bucket or local mirror.
//!
//! The byte accessor must be rooted at `pmc-oa-opendata` (or a mirror preserving
//! its object paths). Metadata without a PDF produces no row. Discovery and
//! article reads are sequential; listing order and explicit duplicates survive.

mod accessor;
mod input;
mod metadata;
mod reader;
mod request;

pub use accessor::PmcAccessor;
pub use request::{PmcInput, PmcLimits, PmcRequest};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use open_arrow_access_core::{Capability, Error, ErrorKind};
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};

/// Fixed PDF-snapshot schema, compatible with the reference PMC source.
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
                    Field::new("pdf_bytes", DataType::LargeBinary, false),
                    Field::new("raw_metadata_json", DataType::LargeUtf8, true),
                ],
                HashMap::from([("pmc_arrow.row_kind".into(), "pdf_snapshot".into())]),
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

fn valid_path(path: &str) -> bool {
    !path.is_empty()
        && path.trim() == path
        && !path.contains(['\\', '?', '#'])
        && !path.contains("://")
        && !path.chars().any(char::is_control)
        && !path.split('/').any(|part| matches!(part, "" | "." | ".."))
}
