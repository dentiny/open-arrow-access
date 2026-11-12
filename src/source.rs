//! Optional dynamic source/format resolution over the typed access APIs.
//!
//! Backends, formats, and providers are enabled independently. Applications
//! register handlers to select identity, pagination, and record representation.
use crate as access;
#[cfg(any(
    feature = "format-parquet",
    feature = "format-json",
    feature = "format-csv",
    feature = "format-xml",
    feature = "format-atif",
    feature = "format-warc",
    feature = "decompression-gzip",
    feature = "decompression-bz2",
    feature = "decompression-lz4",
    feature = "decompression-zstd"
))]
use access::ReaderFactoryExt;
use access::storage::{OpenDalAccessor, opendal};
use access::{
    Accessor, ByteReadOptions, ByteReadRequest, ByteReader, Error, ErrorKind, ReadOptions,
    ReadRequest, RecordReader, Result, async_trait,
};
use std::{collections::HashMap, path::Path, sync::Arc};
use url::Url;

/// A source selection independent of any application's SQL or FFI interface.
#[derive(Clone, Default)]
pub struct SourceRequest {
    pub locator: String,
    pub source: Option<String>,
    pub format: Option<String>,
    pub storage_options: HashMap<String, String>,
    pub service_options: HashMap<String, String>,
    pub format_options: HashMap<String, String>,
    pub options: ReadOptions,
    pub byte_options: ByteReadOptions,
}
impl SourceRequest {
    pub fn new(locator: impl Into<String>) -> Self {
        Self {
            locator: locator.into(),
            ..Self::default()
        }
    }
    pub fn read_request(&self) -> ReadRequest {
        ReadRequest::new(self.locator.clone())
            .with_options(self.options.clone())
            .with_byte_options(self.byte_options)
    }
}
/// Service handlers can supply native Arrow records or original bytes for decoding.
pub enum SourceInput {
    Bytes(Box<dyn ByteReader>),
    Records(Box<dyn RecordReader>),
}
#[async_trait]
pub trait SourceHandler: Send + Sync {
    async fn open(&self, request: &SourceRequest) -> Result<SourceInput>;
}
#[async_trait]
pub trait FormatHandler: Send + Sync {
    async fn decode(
        &self,
        input: Box<dyn ByteReader>,
        request: &SourceRequest,
    ) -> Result<Box<dyn RecordReader>>;
}
/// Unified entry point. Unknown providers/formats fail; they are never guessed as JSON.
/// Storage schemes are resolved by OpenDAL's enabled-service registry.
pub struct SourceRegistry {
    sources: HashMap<String, Arc<dyn SourceHandler>>,
    formats: HashMap<String, Arc<dyn FormatHandler>>,
    suffixes: HashMap<String, String>,
}
impl SourceRegistry {
    pub fn empty() -> Self {
        Self {
            sources: HashMap::new(),
            formats: HashMap::new(),
            suffixes: HashMap::new(),
        }
    }
    pub fn register_source(&mut self, name: &str, handler: Arc<dyn SourceHandler>) {
        self.sources.insert(name.to_ascii_lowercase(), handler);
    }
    pub fn register_format(
        &mut self,
        name: &str,
        suffixes: &[&str],
        handler: Arc<dyn FormatHandler>,
    ) {
        let name = name.to_ascii_lowercase();
        self.formats.insert(name.clone(), handler);
        for suffix in suffixes {
            self.suffixes
                .insert(suffix.to_ascii_lowercase(), name.clone());
        }
    }
    pub async fn open(&self, request: SourceRequest) -> Result<Box<dyn RecordReader>> {
        if request.locator.is_empty() {
            return Err(invalid("source locator must not be empty"));
        }
        let scheme = request
            .source
            .clone()
            .unwrap_or_else(|| {
                request
                    .locator
                    .split_once("://")
                    .map(|(s, _)| s.to_ascii_lowercase())
                    .unwrap_or_else(|| "file".into())
            })
            .to_ascii_lowercase();
        let input = if let Some(source) = self.sources.get(&scheme) {
            source.open(&request).await?
        } else {
            if request.source.is_some() {
                return Err(unsupported("source handler is not registered"));
            }
            StorageSource.open(&request).await?
        };
        match input {
            SourceInput::Records(reader) => {
                if request.format.is_some() || !request.format_options.is_empty() {
                    return Err(invalid("native record sources do not accept a file format"));
                }
                Ok(reader)
            }
            SourceInput::Bytes(input) => {
                let path = locator_path(
                    input
                        .metadata()
                        .source
                        .as_ref()
                        .map(|s| s.locator())
                        .unwrap_or(&request.locator),
                )?;
                let decoded_path = strip_compression(&path);
                let format = request
                    .format
                    .as_ref()
                    .map(|f| f.to_ascii_lowercase())
                    .or_else(|| {
                        decoded_path
                            .rsplit('.')
                            .next()
                            .and_then(|s| self.suffixes.get(&s.to_ascii_lowercase()).cloned())
                    })
                    .ok_or_else(|| {
                        invalid("cannot infer file format; supply an explicit format")
                    })?;
                let handler = self.formats.get(&format).ok_or_else(|| {
                    unsupported("format is not registered or its feature is disabled")
                })?;
                let input = decompress(input, &path, &request).await?;
                handler.decode(input, &request).await
            }
        }
    }
}
impl Default for SourceRegistry {
    fn default() -> Self {
        #[allow(unused_mut)]
        let mut registry = Self::empty();
        #[cfg(feature = "format-parquet")]
        registry.register_format(
            "parquet",
            &["parquet", "pq"],
            Arc::new(BuiltinFormat::Parquet),
        );
        #[cfg(feature = "format-json")]
        {
            registry.register_format(
                "json",
                &["json", "jsonl", "ndjson"],
                Arc::new(BuiltinFormat::Json),
            );
            registry.register_format("jsonl", &[], Arc::new(BuiltinFormat::Json));
            registry.register_format("ndjson", &[], Arc::new(BuiltinFormat::Json));
        }
        #[cfg(feature = "format-csv")]
        {
            registry.register_format("csv", &["csv"], Arc::new(BuiltinFormat::Csv(false)));
            registry.register_format("tsv", &["tsv"], Arc::new(BuiltinFormat::Csv(true)));
        }
        #[cfg(feature = "format-xml")]
        registry.register_format("xml", &["xml"], Arc::new(BuiltinFormat::Xml));
        #[cfg(feature = "format-atif")]
        {
            registry.register_format("atif", &["atif"], Arc::new(BuiltinFormat::Atif(false)));
            registry.register_format("atif-jsonl", &[], Arc::new(BuiltinFormat::Atif(true)));
        }
        #[cfg(feature = "format-warc")]
        registry.register_format("warc", &["warc"], Arc::new(BuiltinFormat::Warc));
        #[cfg(feature = "service-huggingface")]
        registry.register_source("hf", Arc::new(HuggingFaceSource));
        registry
    }
}
fn invalid(message: &str) -> Error {
    Error::new(ErrorKind::InvalidInput, "resolve_source", message)
}
fn unsupported(message: &str) -> Error {
    Error::new(ErrorKind::Unsupported, "resolve_source", message)
}
fn io_error(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::new(ErrorKind::Io, "open_source", error.to_string()).with_source(error)
}
fn storage_error(error: opendal::Error) -> Error {
    Error::new(
        access::storage::opendal_error_kind(error.kind()),
        "open_source",
        error.to_string(),
    )
    .with_source(error)
}
fn locator_path(locator: &str) -> Result<String> {
    if locator.contains("://") {
        Ok(Url::parse(locator)
            .map_err(|_| invalid("invalid source URI"))?
            .path()
            .into())
    } else {
        Ok(locator.into())
    }
}
fn strip_compression(path: &str) -> &str {
    for suffix in [".gz", ".bz2", ".lz4", ".zst", ".zstd"] {
        if let Some(path) = path.strip_suffix(suffix) {
            return path;
        }
    }
    path
}
struct StorageSource;
#[async_trait]
impl SourceHandler for StorageSource {
    async fn open(&self, request: &SourceRequest) -> Result<SourceInput> {
        if !request.service_options.is_empty() {
            return Err(invalid("storage sources do not accept service options"));
        }
        // Explicit initialization is needed when this code is linked as a static library.
        opendal::install_default();
        let mut config = request.storage_options.clone();
        let (operator, path) = if !request.locator.contains("://")
            || request.locator.starts_with("file://")
        {
            let path = if request.locator.starts_with("file://") {
                Url::parse(&request.locator)
                    .map_err(|_| invalid("invalid file URI"))?
                    .to_file_path()
                    .map_err(|_| invalid("file URI must reference a local path"))?
            } else {
                Path::new(&request.locator).to_path_buf()
            };
            let path = std::path::absolute(path).map_err(io_error)?;
            if config.contains_key("root") {
                return Err(invalid("file URI/path already determines the storage root"));
            }
            config.insert(
                "root".into(),
                path.parent()
                    .ok_or_else(|| invalid("source must name a file"))?
                    .to_string_lossy()
                    .into(),
            );
            let name = path
                .file_name()
                .ok_or_else(|| invalid("source must name a file"))?
                .to_str()
                .ok_or_else(|| invalid("source path must be UTF-8"))?
                .to_owned();
            (
                opendal::Operator::via_iter("fs", config).map_err(storage_error)?,
                name,
            )
        } else {
            let mut url =
                Url::parse(&request.locator).map_err(|_| invalid("invalid source URI"))?;
            if url.fragment().is_some() || !url.username().is_empty() || url.password().is_some() {
                return Err(invalid(
                    "URI fragments and embedded credentials are unsupported",
                ));
            }
            let scheme = url.scheme().to_owned();
            let path = url.path().trim_start_matches('/').to_owned();
            if matches!(scheme.as_str(), "http" | "https") {
                if config.contains_key("endpoint") || config.contains_key("root") {
                    return Err(invalid("HTTP URI already determines endpoint and root"));
                }
                if url.query().is_some() {
                    return Err(unsupported(
                        "HTTP query URLs require a custom source handler",
                    ));
                }
                let path = percent_encoding::percent_decode_str(&path)
                    .decode_utf8()
                    .map_err(|_| invalid("HTTP object path must be UTF-8"))?
                    .into_owned();
                url.set_path("/");
                url.set_query(None);
                config.insert("endpoint".into(), url.as_str().trim_end_matches('/').into());
                (
                    opendal::Operator::via_iter("http", config).map_err(storage_error)?,
                    path,
                )
            } else {
                if url.query().is_some() {
                    return Err(invalid(
                        "configure storage through storage_options rather than URI query parameters",
                    ));
                }
                if config.contains_key("root") {
                    return Err(invalid("object URI already determines its root"));
                }
                url.set_path("/");
                let uri = opendal::OperatorUri::new(url.as_str(), config).map_err(storage_error)?;
                (
                    opendal::Operator::from_uri(uri).map_err(storage_error)?,
                    percent_encoding::percent_decode_str(&path)
                        .decode_utf8()
                        .map_err(|_| invalid("object path must be UTF-8"))?
                        .into_owned(),
                )
            }
        };
        let input = OpenDalAccessor::new(operator)
            .open(ByteReadRequest::new(path).with_options(request.byte_options))
            .await?;
        Ok(SourceInput::Bytes(input))
    }
}
async fn decompress(
    input: Box<dyn ByteReader>,
    path: &str,
    request: &SourceRequest,
) -> Result<Box<dyn ByteReader>> {
    let _ = request;
    macro_rules! codec {
        ($feature:literal, $codec:expr) => {{
            #[cfg(feature=$feature)]
            {
                return access::decompression::DecompressionFactory::new($codec)
                    .open_input(input, ByteReadRequest::from(&request.read_request()))
                    .await;
            }
            #[cfg(not(feature=$feature))]
            {
                return Err(unsupported("compression codec feature is disabled"));
            }
        }};
    }
    if path.ends_with(".gz") {
        codec!("decompression-gzip", access::decompression::gzip::Gzip);
    }
    if path.ends_with(".bz2") {
        codec!("decompression-bz2", access::decompression::bz2::Bzip2);
    }
    if path.ends_with(".lz4") {
        codec!("decompression-lz4", access::decompression::lz4::Lz4);
    }
    if path.ends_with(".zst") || path.ends_with(".zstd") {
        codec!("decompression-zstd", access::decompression::zstd::Zstd);
    }
    Ok(input)
}
#[cfg(any(
    feature = "format-parquet",
    feature = "format-json",
    feature = "format-csv",
    feature = "format-xml",
    feature = "format-atif",
    feature = "format-warc"
))]
enum BuiltinFormat {
    #[cfg(feature = "format-parquet")]
    Parquet,
    #[cfg(feature = "format-json")]
    Json,
    #[cfg(feature = "format-csv")]
    Csv(bool),
    #[cfg(feature = "format-xml")]
    Xml,
    #[cfg(feature = "format-atif")]
    Atif(bool),
    #[cfg(feature = "format-warc")]
    Warc,
}
#[cfg(any(
    feature = "format-parquet",
    feature = "format-json",
    feature = "format-csv",
    feature = "format-xml",
    feature = "format-atif",
    feature = "format-warc"
))]
#[async_trait]
impl FormatHandler for BuiltinFormat {
    async fn decode(
        &self,
        input: Box<dyn ByteReader>,
        source: &SourceRequest,
    ) -> Result<Box<dyn RecordReader>> {
        let request = source.read_request();
        let _ = (&input, &request);
        #[allow(unreachable_patterns)]
        match self {
            #[cfg(feature = "format-parquet")]
            Self::Parquet => {
                check_options(&source.format_options, &[])?;
                access::file_format::parquet::ParquetFormat
                    .open_input(input, request)
                    .await
            }
            #[cfg(feature = "format-json")]
            Self::Json => {
                check_options(&source.format_options, &[])?;
                access::file_format::json::JsonFormat::infer()
                    .open_input(input, request)
                    .await
            }
            #[cfg(feature = "format-csv")]
            Self::Csv(tsv) => {
                use access::file_format::csv::{CsvDialect, CsvFileFormat, CsvInferenceOptions};
                check_options(&source.format_options, &["header"])?;
                let header = source
                    .format_options
                    .get("header")
                    .map(|s| {
                        s.parse::<bool>()
                            .map_err(|_| invalid("header must be true or false"))
                    })
                    .transpose()?;
                CsvFileFormat::infer()
                    .with_dialect(CsvDialect {
                        delimiter: if *tsv { b'\t' } else { b',' },
                        ..Default::default()
                    })
                    .with_inference_options(CsvInferenceOptions {
                        has_header: header,
                        ..Default::default()
                    })
                    .open_input(input, request)
                    .await
            }
            #[cfg(feature = "format-xml")]
            Self::Xml => {
                check_options(&source.format_options, &["record_path"])?;
                let path = source
                    .format_options
                    .get("record_path")
                    .ok_or_else(|| invalid("XML requires format_options.record_path"))?;
                access::file_format::xml::XmlFormat::new(path)
                    .open_input(input, request)
                    .await
            }
            #[cfg(feature = "format-atif")]
            Self::Atif(lines) => {
                check_options(&source.format_options, &[])?;
                use access::file_format::atif::AtifFormat;
                (if *lines {
                    AtifFormat::JsonLines
                } else {
                    AtifFormat::Json
                })
                .open_input(input, request)
                .await
            }
            #[cfg(feature = "format-warc")]
            Self::Warc => {
                check_options(&source.format_options, &[])?;
                access::file_format::warc::WarcArrowFormat
                    .open_input(input, request)
                    .await
            }
            _ => Err(unsupported("no format decoder is enabled")),
        }
    }
}
#[cfg(any(
    feature = "format-parquet",
    feature = "format-json",
    feature = "format-csv",
    feature = "format-xml",
    feature = "format-atif",
    feature = "format-warc",
    feature = "service-huggingface"
))]
fn check_options(options: &HashMap<String, String>, allowed: &[&str]) -> Result<()> {
    if options.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(invalid("unrecognized format or service option"));
    }
    Ok(())
}
#[cfg(feature = "service-huggingface")]
struct HuggingFaceSource;
#[cfg(feature = "service-huggingface")]
#[async_trait]
impl SourceHandler for HuggingFaceSource {
    async fn open(&self, request: &SourceRequest) -> Result<SourceInput> {
        use access::service::huggingface::{
            HuggingFaceDatasetRequest, HuggingFaceOptions, HuggingFaceRepository,
            HuggingFaceService,
        };
        if !request.storage_options.is_empty() {
            return Err(invalid(
                "HF sources use service_options, not storage_options",
            ));
        }
        check_options(
            &request.service_options,
            &[
                "config",
                "split",
                "revision",
                "token",
                "endpoint",
                "path",
                "allow_partial",
            ],
        )?;
        let url = Url::parse(&request.locator).map_err(|_| invalid("invalid HF URI"))?;
        if url.host_str() != Some("datasets")
            || url.query().is_some()
            || url.fragment().is_some()
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(invalid(
                "HF URI must be hf://datasets/<owner>/<repository>; configure selection in service_options",
            ));
        }
        let repository = url.path().trim_matches('/');
        if repository.split('/').count() != 2 || repository.split('/').any(str::is_empty) {
            return Err(invalid(
                "HF URI must name exactly an owner and repository; use service_options.path for a file",
            ));
        }
        let options = &request.service_options;
        let mut hf = HuggingFaceOptions::default();
        if let Some(endpoint) = options.get("endpoint") {
            hf = hf.with_endpoint(endpoint);
        }
        if let Some(token) = options.get("token") {
            hf = hf.with_token(token);
        }
        if !options.contains_key("path") {
            if request.format.is_some() || !request.format_options.is_empty() {
                return Err(invalid(
                    "HF dataset exports already provide Arrow records; omit format",
                ));
            }
            if !options.contains_key("config") || !options.contains_key("split") {
                return Err(invalid(
                    "HF dataset requires service_options.config and service_options.split",
                ));
            }
        }
        let service = HuggingFaceService::new(hf).await?;
        if let Some(path) = options.get("path") {
            if options.contains_key("config")
                || options.contains_key("split")
                || options.contains_key("allow_partial")
            {
                return Err(invalid(
                    "HF file path cannot be combined with dataset split options",
                ));
            }
            let mut repo = HuggingFaceRepository::new(repository);
            if let Some(revision) = options.get("revision") {
                repo = repo.with_revision(revision);
            }
            let files = service.repository(repo).await?;
            let reader = files
                .open(ByteReadRequest::new(path.clone()).with_options(request.byte_options))
                .await?;
            Ok(SourceInput::Bytes(reader))
        } else {
            let config = options
                .get("config")
                .ok_or_else(|| invalid("HF dataset requires service_options.config"))?;
            let split = options
                .get("split")
                .ok_or_else(|| invalid("HF dataset requires service_options.split"))?;
            let mut dataset = HuggingFaceDatasetRequest::new(repository, config, split)
                .with_options(request.options.clone())
                .with_byte_options(request.byte_options);
            if let Some(revision) = options.get("revision") {
                dataset = dataset.with_revision(revision);
            }
            if let Some(allow) = options.get("allow_partial")
                && allow
                    .parse::<bool>()
                    .map_err(|_| invalid("allow_partial must be true or false"))?
            {
                dataset = dataset.with_partial_exports();
            }
            Ok(SourceInput::Records(service.dataset(dataset).await?))
        }
    }
}
