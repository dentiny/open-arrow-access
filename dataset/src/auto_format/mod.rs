//! File discovery and format detection, followed by native file planning.
//!
//! Enable the `format-*` readers you need. Compression features
//! remain on the decompression crate. Configure storage (including known content
//! lengths) before passing it here; Parquet retains its original range-capable storage.

use crate::{
    directory::DirectoryRequest,
    file_set::{FileSetAccessor, FileSetRequest},
};
#[cfg(any(feature = "format-csv", feature = "format-warc"))]
use open_arrow_access_core::Layer;
use open_arrow_access_core::{
    Accessor, ByteAccessor, Capability, Error, ObjectRef, ReadOptions, ReadOptionsProvider,
    RecordCapability, RecordReader, RecordScanAccessor, Result, ScanPlan, ScanPlanner, SchemaRef,
    async_trait,
};
#[cfg(any(feature = "format-csv", feature = "format-warc"))]
use open_arrow_access_decompression::AutoDecompressionLayer;
use std::sync::Arc;

mod detection;
mod error;
mod validation;
pub use detection::{DetectedFormat, detect_format, select_format};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResolvedKind {
    Parquet,
    CsvOrTsv,
    Json,
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
}
impl<B> AutoFormatAccessor<B> {
    pub fn new(storage: B) -> Self {
        Self {
            storage: Arc::new(storage),
            format: None,
            schema: None,
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
}

impl<B: ByteAccessor + 'static> AutoFormatAccessor<B> {
    #[cfg(any(feature = "format-csv", feature = "format-warc"))]
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

    /// Resolve selected files without decoding Arrow records. Explicit file lists
    /// perform no I/O. JSON support is added separately.
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
                use open_arrow_access_file_format::csv::CsvAccessor;
                let reader = match &self.schema {
                    Some(schema) => CsvAccessor::new(self.decoded(), schema.clone()),
                    None => CsvAccessor::infer(self.decoded()),
                };
                let reader = match self.format {
                    Some(DetectedFormat::Csv) => reader.with_delimiter(b','),
                    Some(DetectedFormat::Tsv) => reader.with_delimiter(b'\t'),
                    _ => reader,
                };
                Ok(Arc::new(reader))
            }
            #[cfg(feature = "format-warc")]
            ResolvedKind::Warc => Ok(Arc::new(
                open_arrow_access_file_format::warc::WarcArrowAccessor::new(self.decoded()),
            )),
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
