//! File discovery and format detection, followed by native file planning.
//!
//! Enable the `format-*` readers you need. Compression features
//! remain on the decompression crate. Configure storage (including known content
//! lengths) before passing it here; Parquet retains its original range-capable storage.
//!
//! Automatic routing is limited to the built-in [`DetectedFormat`] and
//! [`ResolvedKind`] variants and their enabled readers. This version reads CSV,
//! TSV, Parquet, WARC, ordinary JSON, and ATIF (with their reader features).
//! Applications can implement `open_arrow_access_core::ReaderFactory` and use
//! `open_arrow_access_file_format::FileAccessor::from_format` for custom formats,
//! including with [`FileSetAccessor`] for multiple files. Custom formats are not
//! registered with or selected by `AutoFormatAccessor`; runtime registration is
//! not currently supported.

mod sniff;

use crate::{
    directory::DirectoryRequest,
    file_set::{FileSetAccessor, FileSetRequest},
};
use futures::{StreamExt, TryStreamExt, stream};
use open_arrow_access_core::Layer;
use open_arrow_access_core::{
    Accessor, ByteAccessor, Capability, Error, ErrorKind, ObjectRef, ReadOptions,
    ReadOptionsProvider, RecordCapability, RecordReader, RecordScanAccessor, Result, ScanPlan,
    ScanPlanner, SchemaRef, async_trait,
};
use open_arrow_access_decompression::AutoDecompressionLayer;
use std::{num::NonZeroUsize, sync::Arc};

// TODO: Support application-defined format registration; see the proposed
// registration design in docs/extending.md and the tracking item in docs/roadmap.md.
mod detection;
mod error;
mod validation;
pub use detection::{DetectedFormat, detect_format, select_format};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum JsonDetection {
    /// Inspect every file's first nonblank line; reject different JSON kinds.
    #[default]
    EveryFile,
    /// Trust the first nonempty file. Later files are not checked for mixed kinds.
    FirstFile,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResolvedKind {
    Parquet,
    CsvOrTsv,
    Json,
    #[cfg(feature = "format-atif")]
    Atif(open_arrow_access_file_format::atif::AtifFormat),
    Warc,
}

#[derive(Clone, Debug)]
pub enum AutoFormatRequest {
    Files(FileSetRequest),
    Directory(DirectoryRequest),
}
impl AutoFormatRequest {
    fn context_object(&self) -> Option<&ObjectRef> {
        match self {
            Self::Files(files) => files.objects.first(),
            Self::Directory(request) => Some(&request.prefix),
        }
    }
}

impl ReadOptionsProvider for AutoFormatRequest {
    fn read_options(&self) -> &ReadOptions {
        match self {
            Self::Files(r) => r.read_options(),
            Self::Directory(r) => r.read_options(),
        }
    }
    fn read_options_mut(&mut self) -> &mut ReadOptions {
        match self {
            Self::Files(r) => r.read_options_mut(),
            Self::Directory(r) => r.read_options_mut(),
        }
    }
}

/// Selected objects retain explicit list order and duplicates; directory requests
/// use the shared sorted, unique discovery order. Known sizes stay in the supplied
/// byte accessor (for example OpenDalAccessor::with_content_lengths).
#[derive(Clone, Debug)]
pub struct ResolvedFormat {
    pub format: ResolvedKind,
    pub objects: Vec<ObjectRef>,
}

pub struct AutoFormatAccessor<B> {
    storage: Arc<B>,
    format: Option<DetectedFormat>,
    schema: Option<SchemaRef>,
    detection: JsonDetection,
    sniff_bytes: NonZeroUsize,
    sniff_concurrency: NonZeroUsize,
}
impl<B> AutoFormatAccessor<B> {
    pub fn new(storage: B) -> Self {
        Self {
            storage: Arc::new(storage),
            format: None,
            schema: None,
            detection: JsonDetection::EveryFile,
            sniff_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
            sniff_concurrency: NonZeroUsize::new(8).unwrap(),
        }
    }
    /// Read every selected object as this format, without suffix filtering.
    /// By default, detect suffixes and require one format. Explicit CSV uses commas;
    /// explicit TSV uses tabs, regardless of suffix.
    pub fn with_format(mut self, format: DetectedFormat) -> Self {
        self.format = Some(format);
        self
    }
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }
    pub fn with_schema_opt(mut self, schema: Option<SchemaRef>) -> Self {
        self.schema = schema;
        self
    }
    pub fn with_json_detection(mut self, detection: JsonDetection) -> Self {
        self.detection = detection;
        self
    }
    /// Independent preflight budget; record execution layers do not govern discovery.
    pub fn with_json_sniff_limits(
        mut self,
        bytes: NonZeroUsize,
        concurrency: NonZeroUsize,
    ) -> Self {
        self.sniff_bytes = bytes;
        self.sniff_concurrency = concurrency;
        self
    }
}

impl<B: ByteAccessor + 'static> AutoFormatAccessor<B> {
    fn decoded(&self) -> Arc<dyn ByteAccessor> {
        Arc::new(AutoDecompressionLayer::new().layer(self.storage.clone()))
    }

    async fn files(&self, request: AutoFormatRequest) -> Result<FileSetRequest> {
        match request {
            AutoFormatRequest::Files(files) => Ok(files),
            AutoFormatRequest::Directory(request) => {
                crate::directory::accessor::resolve_files(&self.storage, request).await
            }
        }
    }

    /// Resolve format and JSON kind without decoding Arrow records. Non-JSON
    /// explicit lists perform no I/O. JSON reads only a bounded first nonblank line.
    pub async fn resolve(&self, request: AutoFormatRequest) -> Result<ResolvedFormat> {
        self.prepare(request).await.map(|(resolved, _, _)| resolved)
    }

    async fn prepare(
        &self,
        request: AutoFormatRequest,
    ) -> Result<(ResolvedFormat, FileSetRequest, Arc<dyn RecordScanAccessor>)> {
        let context = request.context_object().cloned();
        async {
            let files = self.files(request).await?;
            let resolved = self.resolve_files(&files).await?;
            let reader = self.reader(resolved.format, resolved.objects.first())?;
            Ok((resolved, files, reader))
        }
        .await
        .map_err(|error: Error| error.with_object(context.as_ref()))
    }

    async fn resolve_files(&self, request: &FileSetRequest) -> Result<ResolvedFormat> {
        let (detected, objects) = match self.format {
            Some(format) => {
                validation::validate_files(&request.objects)?;
                (format, request.objects.clone())
            }
            None => select_format(&request.objects)?,
        };
        let format = match detected {
            DetectedFormat::Parquet => ResolvedKind::Parquet,
            DetectedFormat::Csv | DetectedFormat::Tsv => ResolvedKind::CsvOrTsv,
            DetectedFormat::Json => ResolvedKind::Json,
            DetectedFormat::Warc => ResolvedKind::Warc,
        };
        if format == ResolvedKind::Json && !cfg!(feature = "format-json") {
            return Err(error::resolve(
                ErrorKind::Unsupported,
                "JSON reader feature is disabled",
                objects.first(),
            ));
        }
        if format == ResolvedKind::Json {
            let decoded = self.decoded();
            let concurrency = match self.detection {
                JsonDetection::EveryFile => self.sniff_concurrency.get(),
                JsonDetection::FirstFile => 1,
            };
            let probes = stream::iter(objects.iter().cloned().enumerate())
                .map(|(index, object)| {
                    let decoded = decoded.clone();
                    async move {
                        let kind = sniff::probe(
                            decoded.as_ref(),
                            &object,
                            request.byte_options,
                            self.sniff_bytes.get(),
                        )
                        .await?;
                        Ok::<_, Error>(kind.map(|kind| (index, object, kind)))
                    }
                })
                .buffered(concurrency)
                .try_filter_map(|sample| futures::future::ready(Ok(sample)));
            futures::pin_mut!(probes);
            let Some((index, example, kind)) = probes.try_next().await? else {
                return Err(error::resolve(
                    ErrorKind::NotFound,
                    "JSON selection contains only empty files",
                    objects.first(),
                ));
            };
            let mut retained = vec![example.clone()];
            if self.detection == JsonDetection::FirstFile {
                retained.extend_from_slice(&objects[index + 1..]);
            } else {
                while let Some((_, object, other)) = probes.try_next().await? {
                    if other != kind {
                        return Err(error::resolve(
                            ErrorKind::InvalidInput,
                            format!(
                                "mixed JSON kinds {kind:?} ({:?}) and {other:?} ({:?})",
                                example.locator(),
                                object.locator()
                            ),
                            Some(&object),
                        ));
                    }
                    retained.push(object);
                }
            }
            return Ok(ResolvedFormat {
                format: kind,
                objects: retained,
            });
        }
        Ok(ResolvedFormat { format, objects })
    }

    fn reader(
        &self,
        kind: ResolvedKind,
        object: Option<&ObjectRef>,
    ) -> Result<Arc<dyn RecordScanAccessor>> {
        validation::validate_schema(self.schema.as_ref(), kind, object)?;
        match kind {
            #[cfg(feature = "format-parquet")]
            ResolvedKind::Parquet => Ok(Arc::new(
                open_arrow_access_file_format::parquet::ParquetAccessor::new(self.storage.clone()),
            )),
            #[cfg(feature = "format-csv")]
            ResolvedKind::CsvOrTsv => {
                use open_arrow_access_core::Layer;
                use open_arrow_access_decompression::AutoDecompressionLayer;
                use open_arrow_access_file_format::csv::CsvAccessor;

                let decoded: Arc<dyn ByteAccessor> =
                    Arc::new(AutoDecompressionLayer::new().layer(self.storage.clone()));
                let reader = match &self.schema {
                    Some(schema) => CsvAccessor::new(decoded, schema.clone()),
                    None => CsvAccessor::infer(decoded),
                };
                let reader = match self.format {
                    Some(DetectedFormat::Csv) => reader.with_delimiter(b','),
                    Some(DetectedFormat::Tsv) => reader.with_delimiter(b'\t'),
                    _ => reader,
                };
                Ok(Arc::new(reader))
            }
            #[cfg(feature = "format-json")]
            ResolvedKind::Json => {
                use open_arrow_access_file_format::json::JsonAccessor;
                Ok(Arc::new(match &self.schema {
                    Some(schema) => JsonAccessor::new(self.decoded(), schema.clone()),
                    None => JsonAccessor::infer(self.decoded()),
                }))
            }
            #[cfg(feature = "format-atif")]
            ResolvedKind::Atif(format) => Ok(Arc::new(
                open_arrow_access_file_format::atif::AtifAccessor::new(self.decoded(), format),
            )),
            #[cfg(feature = "format-warc")]
            ResolvedKind::Warc => {
                use open_arrow_access_core::Layer;
                use open_arrow_access_decompression::AutoDecompressionLayer;
                use open_arrow_access_file_format::warc::WarcArrowAccessor;

                let decoded: Arc<dyn ByteAccessor> =
                    Arc::new(AutoDecompressionLayer::new().layer(self.storage.clone()));
                Ok(Arc::new(WarcArrowAccessor::new(decoded)))
            }
            #[allow(unreachable_patterns)]
            _ => Err(error::disabled_reader(kind, object)),
        }
    }

    async fn plan(&self, request: AutoFormatRequest) -> Result<ScanPlan> {
        let (resolved, files, reader) = self.prepare(request).await?;
        FileSetAccessor::new(reader)
            .with_union_by_name()
            .with_file_planning()
            .plan_scan(FileSetRequest {
                objects: resolved.objects,
                ..files
            })
            .await
    }
}

#[async_trait]
impl<B: ByteAccessor + 'static> Accessor for AutoFormatAccessor<B> {
    type Request = AutoFormatRequest;
    type Reader = dyn RecordReader;
    fn capability(&self) -> RecordCapability {
        Capability {
            read: self.storage.capability().read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
        .with_ordering(open_arrow_access_core::OrderRequirement::PreserveGlobal)
    }
    async fn open(&self, request: AutoFormatRequest) -> Result<Box<dyn RecordReader>> {
        Ok(Box::new(self.plan(request).await?.into_reader()))
    }
}
#[async_trait]
impl<B: ByteAccessor + 'static> ScanPlanner for AutoFormatAccessor<B> {
    async fn plan_scan(&self, request: AutoFormatRequest) -> Result<ScanPlan> {
        self.plan(request).await
    }
}
