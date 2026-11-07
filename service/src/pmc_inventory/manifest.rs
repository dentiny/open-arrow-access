use super::{decode, validation};
use open_arrow_access_core::{Accessor, ByteReadRequest, ByteReader, Result};
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
    pub size: u64,
    #[serde(rename = "MD5checksum")]
    pub checksum: String,
}

pub(super) async fn read<A>(
    source: &A,
    request: ByteReadRequest,
    limit: usize,
) -> Result<(Manifest, String)>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    let mut reader = source.open(request).await?;
    validation::validate_manifest_length(reader.metadata().content_length, limit)?;
    let mut bytes = Vec::new();
    while let Some(chunk) = reader.next().await? {
        validation::validate_manifest_chunk(&chunk, limit - bytes.len())?;
        bytes.extend_from_slice(&chunk);
    }
    let manifest: Manifest = serde_json::from_slice(&bytes)
        .map_err(|error| decode(error.to_string()).with_source(error))?;
    validation::validate_manifest(&manifest)?;
    let raw =
        String::from_utf8(bytes).map_err(|error| decode(error.to_string()).with_source(error))?;
    Ok((manifest, raw))
}
