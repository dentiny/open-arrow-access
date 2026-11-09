//! Storage-independent MECA manifests over the existing ranged ZIP reader.
mod error;
mod parser;
mod resolver;
mod validation;
use open_arrow_access_core::{ByteReader, Bytes, Result, read_all};
use open_arrow_access_decompression::zip::{ZipArchive, ZipLimits};
use std::num::NonZeroUsize;
/// One manifest instance, with its original URI and resolved ZIP member.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MecaFile {
    pub item_id: String,
    pub item_type: Option<String>,
    pub href: String,
    pub media_type: Option<String>,
    pub entry_index: usize,
}
/// A reusable archive session. Unlisted members remain accessible through `archive()`.
#[derive(Clone)]
pub struct MecaArchive {
    archive: ZipArchive,
    manifest: Bytes,
    files: Vec<MecaFile>,
}
impl MecaArchive {
    /// Decode the directory and manifest; open individual payloads on demand.
    pub async fn open(input: Box<dyn ByteReader>, limits: ZipLimits) -> Result<Self> {
        let archive = ZipArchive::open(input, limits).await?;
        let index = resolver::resolve_manifest_index(archive.entries())?;
        let reader = archive
            .open_entry(index, NonZeroUsize::new(65536).unwrap())
            .await?;
        let manifest = Bytes::from(read_all(reader, "read_meca_manifest").await?);
        let parsed = parser::parse_manifest(&manifest)?;
        let files = resolver::resolve_files(parsed, archive.entries())?;
        Ok(Self {
            archive,
            manifest,
            files,
        })
    }
    pub fn archive(&self) -> &ZipArchive {
        &self.archive
    }
    pub fn manifest(&self) -> &Bytes {
        &self.manifest
    }
    pub fn files(&self) -> &[MecaFile] {
        &self.files
    }
}
