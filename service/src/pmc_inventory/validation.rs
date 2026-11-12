use super::{
    error::decode,
    manifest::{InventoryFormat, Manifest},
};
use arrow_array::{Array, cast::AsArray};
use arrow_schema::Fields;
use open_arrow_access_core::{Error, ErrorKind, FieldNameIndex, RecordBatch, Result};
use std::collections::HashSet;

pub(super) fn validate_manifest(manifest: &Manifest) -> Result<()> {
    if manifest.source_bucket != "pmc-oa-opendata"
        || manifest.destination_bucket != "arn:aws:s3:::pmc-oa-opendata"
    {
        return Err(decode(
            "expected a PMC metadata inventory in the pmc-oa-opendata bucket",
        ));
    }
    if manifest.file_format != InventoryFormat::Csv {
        return Err(Error::new(
            ErrorKind::Unsupported,
            "open_pmc_inventory",
            "only CSV inventory manifests are supported",
        ));
    }
    if manifest.version.is_empty() {
        return Err(decode("missing inventory version"));
    }
    validate_fields(manifest.file_schema.fields())?;
    for file in &manifest.files {
        if !file.key.ends_with(".csv.gz") {
            return Err(decode("invalid inventory shard entry"));
        }
    }
    Ok(())
}
fn validate_fields(fields: &Fields) -> Result<()> {
    let mut seen = HashSet::new();
    if fields
        .iter()
        .any(|field| field.name().is_empty() || !seen.insert(field.name().as_str()))
        || ["Bucket", "Key", "LastModifiedDate", "ETag"]
            .iter()
            .any(|required| !seen.contains(required))
    {
        return Err(decode(
            "invalid inventory fileSchema: required PMC fields must be unique",
        ));
    }
    Ok(())
}
fn validate_row(bucket: Option<&str>, key: Option<&str>) -> Result<()> {
    if bucket != Some("pmc-oa-opendata") {
        return Err(decode("inventory row has a missing or unexpected Bucket"));
    }
    if key.is_none_or(str::is_empty) {
        return Err(decode("inventory row has no Key"));
    }
    Ok(())
}

pub(super) fn validate_batch(batch: &RecordBatch, fields: &FieldNameIndex) -> Result<()> {
    let buckets = batch
        .column(fields.index_of("Bucket").unwrap())
        .as_string::<i32>();
    let keys = batch
        .column(fields.index_of("Key").unwrap())
        .as_string::<i32>();
    for row in 0..batch.num_rows() {
        validate_row(
            (!buckets.is_null(row)).then(|| buckets.value(row)),
            (!keys.is_null(row)).then(|| keys.value(row)),
        )?;
    }
    Ok(())
}
