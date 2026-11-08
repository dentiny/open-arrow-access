use super::{error::decode, validation};
use open_arrow_access_core::{Accessor, ByteReadRequest, ByteReader, Result, read_object};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Manifest {
    pub source_bucket: String,
    pub destination_bucket: String,
    pub version: String,
    pub creation_timestamp: String,
    pub file_format: String,
    pub file_schema: String,
    pub files: Vec<Shard>,
}
#[derive(Deserialize)]
pub(super) struct Shard {
    pub key: String,
}

pub(super) async fn read<A>(source: &A, request: ByteReadRequest, limit: usize) -> Result<Manifest>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    let bytes = read_object(source, request, limit, "read_pmc_inventory").await?;
    let manifest: Manifest = serde_json::from_slice(&bytes)
        .map_err(|error| decode(error.to_string()).with_source(error))?;
    validation::validate_manifest(&manifest)?;
    Ok(manifest)
}
