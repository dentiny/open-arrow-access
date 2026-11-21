//! Validation of persistent metadata database values, without database or file I/O.
use super::{BhlMetadataInfo, Table, database::DATABASE_VERSION, error};
use open_arrow_access_core::Result;

pub(super) fn database_destination(exists: bool) -> Result<()> {
    if exists {
        return Err(error::invalid("database path already exists"));
    }
    Ok(())
}

pub(super) fn database_header(count: u64, version: u64) -> Result<()> {
    if count != 1 || version != DATABASE_VERSION {
        return Err(error::invalid(
            "unsupported BHL metadata database version/header",
        ));
    }
    Ok(())
}

pub(super) fn database_manifest(info: &BhlMetadataInfo, expected_tables: usize) -> Result<()> {
    if info.sources.len() != expected_tables {
        return Err(error::invalid("incomplete database source manifest"));
    }
    Ok(())
}

pub(super) fn database_source_schema(
    table: &Table,
    columns: &[(String, String)],
    info: &BhlMetadataInfo,
) -> Result<()> {
    let expected = table
        .fields
        .iter()
        .map(|field| (field.name().clone(), "VARCHAR".to_owned()))
        .collect::<Vec<_>>();
    if columns != expected.as_slice() || !info.sources.contains_key(&table.name) {
        return Err(error::invalid("database source schema mismatch"));
    }
    Ok(())
}

pub(super) fn database_source_rows(actual: u64, expected: u64) -> Result<()> {
    if actual != expected {
        return Err(error::invalid("database source row count mismatch"));
    }
    Ok(())
}
