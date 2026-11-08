use super::{error::decode, validation};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use open_arrow_access_core::{Accessor, ByteReadRequest, ByteReader, Result, read_object};
use serde::{Deserialize, Deserializer, de::Error as _};
use std::sync::Arc;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Manifest {
    pub(super) source_bucket: String,
    pub(super) destination_bucket: String,
    pub(super) version: String,
    /// Unix epoch milliseconds.
    #[serde(deserialize_with = "deserialize_timestamp")]
    pub(super) creation_timestamp: u64,
    pub(super) file_format: InventoryFormat,
    #[serde(deserialize_with = "deserialize_schema")]
    pub(super) file_schema: SchemaRef,
    pub(super) files: Vec<Shard>,
}
#[derive(Deserialize, PartialEq)]
pub(super) enum InventoryFormat {
    #[serde(rename = "CSV")]
    Csv,
    #[serde(other)]
    Other,
}

fn deserialize_timestamp<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<u64, D::Error> {
    String::deserialize(deserializer)?
        .parse()
        .map_err(D::Error::custom)
}

fn deserialize_schema<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<SchemaRef, D::Error> {
    let value = String::deserialize(deserializer)?;
    Ok(Arc::new(Schema::new(
        value
            .split(',')
            .map(|name| Field::new(name.trim(), DataType::Utf8, true))
            .collect::<Vec<_>>(),
    )))
}

#[derive(Deserialize)]
pub(super) struct Shard {
    pub(super) key: String,
}

pub(super) async fn read<A>(source: &A, request: ByteReadRequest) -> Result<Manifest>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    let bytes = read_object(source, request, "read_pmc_inventory").await?;
    let manifest: Manifest = serde_json::from_slice(&bytes)
        .map_err(|error| decode(error.to_string()).with_source(error))?;
    validation::validate_manifest(&manifest)?;
    Ok(manifest)
}
