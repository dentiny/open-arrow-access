//! Published Zenodo records and their attached files.

mod error;
mod validation;

use std::num::NonZeroU64;

use open_arrow_access_core::Result;
use open_arrow_access_storage::OpenDalAccessor;

use serde::Deserialize;

use crate::http_client::files::{HttpFileService, ResolvedFilesPlan};
use crate::{ResolvedFileSet, file_manifest::FileSpec};
use error::map_file_namespace_error;
use validation::validate_record;

/// Connection settings. Construction performs no network requests.
#[derive(Clone, Debug)]
pub struct ZenodoService {
    http: HttpFileService,
}

impl ZenodoService {
    pub fn new() -> Result<Self> {
        Self::with_endpoint("https://zenodo.org")
    }

    /// Select an origin, for example `https://sandbox.zenodo.org`.
    pub fn with_endpoint(endpoint: &str) -> Result<Self> {
        Ok(Self {
            http: HttpFileService::new(
                endpoint,
                "configure_zenodo",
                "resolve_zenodo",
                map_file_namespace_error,
            )?,
        })
    }

    /// Resolve one published record. Membership is fetched once; attachment
    /// bytes are read lazily through the returned accessor.
    pub async fn record(&self, id: NonZeroU64) -> Result<ZenodoAccessor> {
        let url = self
            .http
            .endpoint()
            .join(&format!("api/records/{id}"))
            .unwrap();
        self.http
            .resolve_files(&url, |record: Record| {
                validate_record(&record, id)?;
                // Use this version's canonical content route, never a latest
                // version link or an arbitrary host supplied in metadata.
                ResolvedFilesPlan::new(
                    format!("api/records/{id}/files/"),
                    record
                        .files
                        .iter()
                        .map(|file| FileSpec {
                            name: file.key.clone(),
                            object: format!("{}/content", file.key).into(),
                            content_length: None,
                        })
                        .collect(),
                    ZenodoMetadata {
                        id,
                        files: record.files,
                    },
                )
                .map_err(map_file_namespace_error)
            })
            .await
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
