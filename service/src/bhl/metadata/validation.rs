//! Validation of metadata database values and OCR selections, without database or file I/O.
use super::{BhlMetadataInfo, Table, database::DATABASE_VERSION, error};
use crate::duckdb::TableColumn;
use open_arrow_access_core::{ObjectRef, Result};

pub(super) fn validate_database_destination(exists: bool) -> Result<()> {
    if exists {
        return Err(error::invalid("database path already exists"));
    }
    Ok(())
}

pub(super) fn validate_database_snapshot_id(snapshot_id: &str) -> Result<()> {
    if snapshot_id.trim().is_empty() {
        return Err(error::invalid("database snapshot_id must not be blank"));
    }
    Ok(())
}

pub(super) fn validate_database_header(count: u64, version: u64) -> Result<()> {
    if count != 1 || version != DATABASE_VERSION {
        return Err(error::invalid(
            "unsupported BHL metadata database version/header",
        ));
    }
    Ok(())
}

pub(super) fn validate_managed_source_checksums(info: &serde_json::Value) -> Result<()> {
    // External provenance may omit checksums, but importer-managed manifests
    // must retain the required fields accepted by the original reader.
    if info
        .get("sources")
        .and_then(serde_json::Value::as_object)
        .is_some_and(|sources| {
            sources
                .values()
                .any(|source| source.get("sha256").is_none())
        })
    {
        return Err(error::invalid(
            "managed database source checksum is missing",
        ));
    }
    Ok(())
}

pub(super) fn validate_database_manifest(
    info: &BhlMetadataInfo,
    expected_tables: usize,
) -> Result<()> {
    if info.sources.len() != expected_tables {
        return Err(error::invalid("incomplete database source manifest"));
    }
    Ok(())
}

pub(super) fn validate_database_source_schema(
    table: &Table,
    columns: &[TableColumn],
    info: &BhlMetadataInfo,
) -> Result<()> {
    validate_table_schema(table, columns)?;
    if !info.sources.contains_key(&table.name) {
        return Err(error::invalid("database source schema mismatch"));
    }
    Ok(())
}

pub(super) fn validate_table_schema(table: &Table, columns: &[TableColumn]) -> Result<()> {
    let expected = table
        .fields
        .iter()
        .map(|field| TableColumn {
            name: field.name().clone(),
            data_type: "VARCHAR".to_owned(),
        })
        .collect::<Vec<_>>();
    if columns != expected.as_slice() {
        return Err(error::invalid("database source schema mismatch"));
    }
    Ok(())
}

pub(super) fn validate_database_source_rows(actual: u64, expected: u64) -> Result<()> {
    if actual != expected {
        return Err(error::invalid("database source row count mismatch"));
    }
    Ok(())
}

pub(super) fn validate_canonical_matches(matches: u64, object: &ObjectRef) -> Result<()> {
    if matches != 1 {
        return Err(crate::bhl::error::invalid_pages(format!(
            "expected exactly one OCR key matching authoritative SequenceOrder; found {} for {}",
            matches,
            object.locator()
        ))
        .with_object(Some(object)));
    }
    Ok(())
}
