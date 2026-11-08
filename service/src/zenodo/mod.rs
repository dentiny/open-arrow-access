//! Published Zenodo records and their attached files.

mod error;
mod validation;

use std::num::NonZeroU64;

use open_arrow_access_core::Result;
use open_arrow_access_storage::{NamedFileAccessor, OpenDalAccessor};

use serde::Deserialize;
use url::Url;

use crate::ResolvedFileSet;
use crate::http_client::HttpClient;
use error::membership_error;
use validation::{validate_endpoint, validate_record};

/// Connection settings. Construction performs no network requests.
#[derive(Clone, Debug)]
pub struct ZenodoService {
    endpoint: Url,
    client: HttpClient,
}

impl ZenodoService {
    pub fn new() -> Result<Self> {
        Self::with_endpoint("https://zenodo.org")
    }

    /// Select an origin, for example `https://sandbox.zenodo.org`.
    pub fn with_endpoint(endpoint: &str) -> Result<Self> {
        let endpoint = validate_endpoint(endpoint)?;
        Ok(Self {
            endpoint,
            client: HttpClient::new()?,
        })
    }

    /// Resolve one published record. Membership is fetched once; attachment
    /// bytes are read lazily through the returned accessor.
    pub async fn record(&self, id: NonZeroU64) -> Result<ZenodoAccessor> {
        let url = self.endpoint.join(&format!("api/records/{id}")).unwrap();
        let record: Record = self
            .client
            .json(url.as_str(), None, "resolve_zenodo")
            .await?;
        validate_record(&record, id)?;
        // Use the resolved record's canonical content route, never a concept's
        // latest-version link or an arbitrary host supplied in metadata.
        let root = format!("api/records/{id}/files/");
        let bytes = self.client.accessor(&self.endpoint, &root, &[])?;
        let inner = NamedFileAccessor::new(
            bytes,
            record
                .files
                .iter()
                .map(|file| (file.key.clone(), format!("{}/content", file.key).into()))
                .collect(),
        )
        .map_err(membership_error)?;
        Ok(ResolvedFileSet::new(
            inner,
            ZenodoMetadata {
                id,
                files: record.files,
            },
        ))
    }
}

#[derive(Deserialize)]
struct Record {
    id: u64,
    // Omission is not treated as an empty record (e.g. restricted metadata).
    files: Vec<ZenodoFile>,
}

/// Attachment metadata as supplied by Zenodo. Checksums are exposed, not verified.
#[derive(Clone, Debug, Deserialize)]
pub struct ZenodoFile {
    pub key: String,
    pub size: u64,
    pub checksum: String,
}

/// A resolved record's flat attachment namespace. Compose with existing format
/// and decompression accessors to decode selected files.
pub type ZenodoAccessor = ResolvedFileSet<OpenDalAccessor, ZenodoMetadata>;

/// Metadata retained from the resolved record.
#[derive(Clone, Debug)]
pub struct ZenodoMetadata {
    pub id: NonZeroU64,
    pub files: Vec<ZenodoFile>,
}

impl ZenodoAccessor {
    pub fn id(&self) -> NonZeroU64 {
        self.metadata().id
    }

    pub fn files(&self) -> &[ZenodoFile] {
        &self.metadata().files
    }
}
