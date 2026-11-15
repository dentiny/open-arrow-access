//! arXiv bulk-manifest discovery over caller-configured byte storage.

mod error;
mod manifest;
mod records;
pub use records::{ArxivReadRequest, ArxivRecords, record_schema};
mod validation;

use open_arrow_access_core::{
    Accessor, ByteReadOptions, ByteReadRequest, ByteReader, ErrorKind, Result, read_object,
};
use open_arrow_access_decompression::tar::{TarArchive, TarLimits, TarStream};

use manifest::parse_manifest;
use validation::{validate_archive, validate_archive_length, validate_month};

/// Select an arXiv bulk distribution or discover all available distributions.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ArxivKind {
    /// Probe both manifests, skipping only missing distributions.
    #[default]
    Auto,
    Pdf,
    Source,
}

impl ArxivKind {
    fn prefix(self) -> Option<&'static str> {
        match self {
            Self::Auto => None,
            Self::Pdf => Some("pdf"),
            Self::Source => Some("src"),
        }
    }

    fn root(self) -> Option<&'static str> {
        match self {
            Self::Auto => None,
            Self::Pdf => Some("arXivPDF"),
            Self::Source => Some("arXivSRC"),
        }
    }

    /// The manifest key for a concrete distribution; `Auto` has no single key.
    pub fn manifest_key(self) -> Option<&'static str> {
        match self {
            Self::Auto => None,
            Self::Pdf => Some("pdf/arXiv_pdf_manifest.xml"),
            Self::Source => Some("src/arXiv_src_manifest.xml"),
        }
    }
}

/// One TAR shard in the manifest. MD5 metadata is exposed, not verified.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArxivArchive {
    /// Resolved distribution (`Pdf` or `Source`); `Auto` is invalid for a shard.
    pub kind: ArxivKind,
    pub filename: String,
    /// Year and month encoded as YYMM, e.g. 2410 for October 2024.
    pub month: u16,
    /// One-based TAR chunk number within `month` for the selected `kind`.
    /// Maps to manifest `seq_num` and the filename suffix, e.g. `_003.tar`.
    pub sequence: u32,
    pub size: u64,
    pub num_items: u64,
    pub checksum: String,
}

/// Read an arXiv bulk mirror rooted above `pdf/` and `src/`.
/// Callers configure credentials, region and S3 requester-pays; no listing or API calls are needed.
#[derive(Clone, Debug)]
pub struct ArxivService<A> {
    storage: A,
    tar_limits: TarLimits,
}

impl<A> ArxivService<A> {
    pub fn new(storage: A) -> Self {
        Self {
            storage,
            tar_limits: TarLimits::default(),
        }
    }

    pub fn with_tar_limits(mut self, limits: TarLimits) -> Self {
        self.tar_limits = limits;
        self
    }
}

impl<A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>> ArxivService<A> {
    /// Resolve shards in manifest order, optionally filtering one YYMM month.
    /// Unmatched months return no shards; all manifest rows are validated.
    /// `Auto` probes PDF then source, retaining each manifest's order. Only a
    /// missing manifest is skipped; if both are missing, returns `NotFound`.
    pub async fn manifest(
        &self,
        kind: ArxivKind,
        month: Option<u16>,
        options: ByteReadOptions,
    ) -> Result<Vec<ArxivArchive>> {
        if let Some(month) = month {
            validate_month(month)?;
        }
        let kinds: &[ArxivKind] = match kind {
            ArxivKind::Auto => &[ArxivKind::Pdf, ArxivKind::Source],
            ArxivKind::Pdf => &[ArxivKind::Pdf],
            ArxivKind::Source => &[ArxivKind::Source],
        };
        let mut archives = Vec::new();
        let mut found = false;
        let mut missing = None;
        for &resolved in kinds {
            let bytes = match read_object(
                &self.storage,
                ByteReadRequest::new(resolved.manifest_key().unwrap()).with_options(options),
                "read_arxiv_manifest",
            )
            .await
            {
                Ok(bytes) => bytes,
                Err(error) if kind == ArxivKind::Auto && error.kind() == ErrorKind::NotFound => {
                    missing = Some(error);
                    continue;
                }
                Err(error) => return Err(error),
            };
            archives.extend(parse_manifest(&bytes, resolved, month).await?);
            found = true;
        }
        if !found && let Some(error) = missing {
            return Err(error);
        }
        Ok(archives)
    }

    /// Stream a shard sequentially, yielding the first entry after its header
    /// without a full archive scan or per-entry range reads.
    pub async fn stream(
        &self,
        archive: &ArxivArchive,
        options: ByteReadOptions,
    ) -> Result<TarStream> {
        let input = self.open_archive_input(archive, options).await?;
        Ok(TarStream::new(input, self.tar_limits.clone()))
    }

    /// Open a known-length, ranged TAR for independent entry reads.
    /// Readers can outlive the archive/service and retain source version guarantees.
    pub async fn archive(
        &self,
        archive: &ArxivArchive,
        options: ByteReadOptions,
    ) -> Result<TarArchive> {
        let input = self.open_archive_input(archive, options).await?;
        TarArchive::open(input, self.tar_limits.clone()).await
    }

    async fn open_archive_input(
        &self,
        archive: &ArxivArchive,
        options: ByteReadOptions,
    ) -> Result<Box<dyn ByteReader>> {
        validate_archive(archive)?;
        let input = self
            .storage
            .open(ByteReadRequest::new(archive.filename.clone()).with_options(options))
            .await?;
        validate_archive_length(archive, input.metadata().content_length)?;
        Ok(input)
    }
}

// Native record scans use one partition per manifest shard. Install
// ParallelReadLayer on service.records() to read independent shards concurrently;
// entries inside each TAR are consumed in their physical order.
