//! arXiv bulk-manifest discovery over caller-configured byte storage.

mod error;
mod validation;

use open_arrow_access_core::{
    Accessor, ByteReadOptions, ByteReadRequest, ByteReader, Result, read_object,
};
use open_arrow_access_decompression::tar::{TarArchive, TarLimits, TarStream};

use validation::{parse_manifest, validate_archive, validate_archive_length, validate_month};

/// The two separately published arXiv bulk distributions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArxivKind {
    Pdf,
    Source,
}

impl ArxivKind {
    fn prefix(self) -> &'static str {
        match self {
            Self::Pdf => "pdf",
            Self::Source => "src",
        }
    }
    fn root(self) -> &'static str {
        match self {
            Self::Pdf => "arXivPDF",
            Self::Source => "arXivSRC",
        }
    }
    pub fn manifest_key(self) -> &'static str {
        match self {
            Self::Pdf => "pdf/arXiv_pdf_manifest.xml",
            Self::Source => "src/arXiv_src_manifest.xml",
        }
    }
}

/// One TAR shard in the manifest. MD5 metadata is exposed, not verified.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArxivArchive {
    pub kind: ArxivKind,
    pub filename: String,
    pub month: String,
    pub sequence: u32,
    pub size: u64,
    pub num_items: u64,
    pub first_item: String,
    pub last_item: String,
    pub checksum: String,
}

/// Read an arXiv bulk mirror rooted above `pdf/` and `src/`.
/// Callers configure storage credentials, S3 requester-pays and region. No
/// listing permission or arXiv API requests are required. Manifest order is kept.
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
    /// Resolve a PDF/source manifest, optionally filtering one YYMM month.
    /// An absent month is a valid empty selection. All manifest rows are validated.
    pub async fn manifest(
        &self,
        kind: ArxivKind,
        month: Option<&str>,
        options: ByteReadOptions,
    ) -> Result<Vec<ArxivArchive>> {
        if let Some(month) = month {
            validate_month(month)?;
        }
        let bytes = read_object(
            &self.storage,
            ByteReadRequest::new(kind.manifest_key()).with_options(options),
            "read_arxiv_manifest",
        )
        .await?;
        parse_manifest(&bytes, kind, month)
    }

    /// Consume a shard incrementally over its sequential byte stream. This is
    /// the preferred bulk path: the first entry is available after its header,
    /// without scanning every archive header or issuing per-entry range reads.
    pub async fn stream(
        &self,
        archive: &ArxivArchive,
        options: ByteReadOptions,
    ) -> Result<TarStream> {
        validate_archive(archive)?;
        let input = self
            .storage
            .open(ByteReadRequest::new(archive.filename.clone()).with_options(options))
            .await?;
        validate_archive_length(archive, input.metadata().content_length)?;
        Ok(TarStream::new(input, self.tar_limits.clone()))
    }

    /// Inspect a selected TAR using exact ranges. Each entry can be streamed
    /// independently, and readers can outlive the archive/service. This requires
    /// a ranged, known-length input. The source's version guarantees are retained.
    pub async fn archive(
        &self,
        archive: &ArxivArchive,
        options: ByteReadOptions,
    ) -> Result<TarArchive> {
        validate_archive(archive)?;
        let input = self
            .storage
            .open(ByteReadRequest::new(archive.filename.clone()).with_options(options))
            .await?;
        validate_archive_length(archive, input.metadata().content_length)?;
        TarArchive::open(input, self.tar_limits.clone()).await
    }
}
