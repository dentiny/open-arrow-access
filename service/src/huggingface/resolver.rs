use std::collections::HashSet;

use glob::{MatchOptions, Pattern};
use http::{Request, StatusCode};
use open_arrow_access_core::{Error, ErrorKind, ObjectRef, Result};
use opendal::{Buffer, OperationContext, raw::oio::ReadStream};
use serde::Deserialize;
use url::Url;

use super::{
    HuggingFaceAccessor, HuggingFaceOptions, HuggingFaceRepository, accessor::validate_path,
    map_error,
};

const MAX_METADATA_BYTES: usize = 32 * 1024 * 1024;

/// Select one explicitly declared dataset-card configuration and split.
#[derive(Clone, Debug)]
pub struct HuggingFaceSelection {
    pub repository: String,
    pub revision: String,
    pub config: String,
    pub split: String,
}

impl HuggingFaceSelection {
    pub fn new(
        repository: impl Into<String>,
        config: impl Into<String>,
        split: impl Into<String>,
    ) -> Self {
        Self {
            repository: repository.into(),
            revision: "main".into(),
            config: config.into(),
            split: split.into(),
        }
    }

    pub fn with_revision(mut self, revision: impl Into<String>) -> Self {
        self.revision = revision.into();
        self
    }
}

/// Resolved native files, bound to one immutable repository commit.
///
/// File references are relative to `accessor`. Explicit pattern order is kept;
/// matches within a pattern are sorted and overlapping matches are deduplicated.
/// Feed individual objects to existing format accessors. Multi-file Arrow scans
/// belong to the separate dataset adapter.
#[derive(Clone, Debug)]
pub struct HuggingFaceDataset {
    pub selection: HuggingFaceSelection,
    pub commit: String,
    pub objects: Vec<ObjectRef>,
    pub accessor: HuggingFaceAccessor,
}

/// Provider entry point for repository access and dataset selection.
#[derive(Clone, Debug)]
pub struct HuggingFaceService {
    pub(super) options: HuggingFaceOptions,
    pub(super) context: OperationContext,
}

impl HuggingFaceService {
    /// Resolve connection settings and cached credentials on the blocking pool.
    /// Does not contact the Hub until discovery or file reading is requested.
    pub async fn new(mut options: HuggingFaceOptions) -> Result<Self> {
        tokio::task::spawn_blocking(move || {
            options.resolve_environment();
            endpoint(&options.endpoint)?;
            if let Some(token) = &options.token
                && (token.is_empty()
                    || http::HeaderValue::from_str(&format!("Bearer {token}")).is_err())
            {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "configure_huggingface",
                    "invalid Hub authentication token",
                ));
            }
            opendal::install_default();
            Ok(Self {
                options,
                context: OperationContext::new(),
            })
        })
        .await
        .map_err(|e| {
            Error::new(
                ErrorKind::Io,
                "configure_huggingface",
                "initialization task failed",
            )
            .with_source(e)
        })?
    }

    /// Access explicit files in a dataset repository. A floating revision is
    /// read as supplied; use `resolve` to pin config/split discovery to a commit.
    pub async fn repository(
        &self,
        repository: HuggingFaceRepository,
    ) -> Result<HuggingFaceAccessor> {
        repository.validate()?;
        let mut builder = opendal::services::Hf::default()
            .repo_type("dataset")
            .repo_id(&repository.repository)
            .revision(&repository.revision)
            .root(&repository.root)
            .endpoint(&self.options.endpoint);
        if let Some(token) = &self.options.token {
            builder = builder.token(token);
        }
        if self.options.http_only {
            builder = builder.download_mode("http");
        }
        HuggingFaceAccessor::new(builder, self.context.clone()).await
    }

    /// Resolve declared `configs[].data_files` (paths, lists and globs), including
    /// `data_dir`, against the selected revision's card and repository manifest.
    /// Script execution, inferred splits and converted Viewer exports are not
    /// silently substituted for a missing declaration.
    pub async fn resolve(&self, selection: HuggingFaceSelection) -> Result<HuggingFaceDataset> {
        let repository =
            HuggingFaceRepository::new(&selection.repository).with_revision(&selection.revision);
        repository.validate()?;
        if selection.config.is_empty() || selection.split.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "resolve_huggingface",
                "config and split must be specified",
            ));
        }
        let bytes = self.info(&repository).await?;
        let requested = selection.clone();
        // Parsing and glob expansion can scan a large repository manifest.
        // Keep that CPU work off the async runtime's worker threads.
        let (commit, objects) = tokio::task::spawn_blocking(move || {
            let info: RepositoryInfo = serde_json::from_slice(&bytes).map_err(|e| {
                Error::new(
                    ErrorKind::Decode,
                    "resolve_huggingface",
                    "invalid Hub repository metadata",
                )
                .with_source(e)
            })?;
            if info.sha.len() != 40 || !info.sha.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(Error::new(
                    ErrorKind::Decode,
                    "resolve_huggingface",
                    "Hub metadata did not contain a commit SHA",
                ));
            }
            let objects = resolve_files(&info, &requested)?;
            Ok((info.sha, objects))
        })
        .await
        .map_err(|e| {
            Error::new(
                ErrorKind::Io,
                "resolve_huggingface",
                "metadata resolution task failed",
            )
            .with_source(e)
        })??;
        let accessor = self.repository(repository.with_revision(&commit)).await?;
        Ok(HuggingFaceDataset {
            selection,
            commit,
            objects,
            accessor,
        })
    }

    async fn info(&self, repository: &HuggingFaceRepository) -> Result<Vec<u8>> {
        let mut url = endpoint(&self.options.endpoint)?;
        {
            let mut path = url.path_segments_mut().map_err(|_| {
                Error::new(
                    ErrorKind::InvalidInput,
                    "configure_huggingface",
                    "invalid Hub endpoint",
                )
            })?;
            path.pop_if_empty().push("api").push("datasets");
            for part in repository.repository.split('/') {
                path.push(part);
            }
            path.push("revision").push(&repository.revision);
        }
        let mut request = Request::get(url.as_str());
        if let Some(token) = &self.options.token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        let request = request.body(Buffer::new()).map_err(|_| {
            Error::new(
                ErrorKind::InvalidInput,
                "configure_huggingface",
                "invalid Hub request configuration",
            )
        })?;
        let response = self
            .context
            .http_transport()
            .fetch(request)
            .await
            .map_err(|e| map_error("resolve_huggingface", &repository.repository, e))?;
        let status = response.status();
        if !status.is_success() {
            let kind = match status {
                StatusCode::NOT_FOUND => opendal::ErrorKind::NotFound,
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                    opendal::ErrorKind::PermissionDenied
                }
                StatusCode::TOO_MANY_REQUESTS => opendal::ErrorKind::RateLimited,
                _ => opendal::ErrorKind::Unexpected,
            };
            return Err(map_error(
                "resolve_huggingface",
                &repository.repository,
                opendal::Error::new(kind, format!("Hub metadata HTTP {status}")),
            ));
        }
        let mut body = response.into_body();
        let mut bytes = Vec::new();
        loop {
            let buffer = body
                .read()
                .await
                .map_err(|e| map_error("resolve_huggingface", &repository.repository, e))?;
            if buffer.is_empty() {
                break;
            }
            if buffer.len() > MAX_METADATA_BYTES - bytes.len() {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "resolve_huggingface",
                    "repository metadata exceeds the 32 MiB discovery limit",
                ));
            }
            for part in buffer {
                bytes.extend_from_slice(&part);
            }
        }
        Ok(bytes)
    }
}

fn endpoint(value: &str) -> Result<Url> {
    let invalid = || {
        Error::new(
            ErrorKind::InvalidInput,
            "configure_huggingface",
            "expected an HTTP(S) Hub endpoint without credentials, query or fragment",
        )
    };
    let url = Url::parse(value).map_err(|_| invalid())?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(invalid());
    }
    Ok(url)
}

#[derive(Deserialize)]
struct RepositoryInfo {
    sha: String,
    siblings: Vec<RepositoryFile>,
    #[serde(default, rename = "cardData")]
    card: serde_json::Value,
}

#[derive(Deserialize)]
struct RepositoryFile {
    rfilename: String,
}

#[derive(Deserialize)]
struct Config {
    #[serde(default)]
    data_dir: String,
    data_files: Option<DataFiles>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum DataFiles {
    Path(String),
    Paths(Vec<String>),
    Splits(Vec<SplitFiles>),
}

#[derive(Deserialize)]
struct SplitFiles {
    split: String,
    path: Paths,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Paths {
    One(String),
    Many(Vec<String>),
}

fn resolve_files(
    info: &RepositoryInfo,
    selection: &HuggingFaceSelection,
) -> Result<Vec<ObjectRef>> {
    let unsupported = || {
        Error::new(
            ErrorKind::Unsupported,
            "resolve_huggingface",
            "selection requires explicit dataset-card configs/data_files; inferred splits and scripts are unsupported",
        )
    };
    let configs = info
        .card
        .get("configs")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(unsupported)?;
    let mut selected = configs.iter().filter(|c| {
        c.get("config_name").and_then(serde_json::Value::as_str) == Some(selection.config.as_str())
    });
    let config = selected.next().ok_or_else(|| {
        Error::new(
            ErrorKind::NotFound,
            "resolve_huggingface",
            "configuration not found",
        )
    })?;
    if selected.next().is_some() {
        return Err(Error::new(
            ErrorKind::Decode,
            "resolve_huggingface",
            "duplicate configuration names",
        ));
    }
    let config: Config = serde_json::from_value(config.clone()).map_err(|e| {
        Error::new(
            ErrorKind::Decode,
            "resolve_huggingface",
            "invalid selected dataset-card configuration",
        )
        .with_source(e)
    })?;
    validate_path(&config.data_dir, true)?;
    let patterns = match config.data_files.ok_or_else(unsupported)? {
        DataFiles::Path(path) if selection.split == "train" => vec![path],
        DataFiles::Paths(paths) if selection.split == "train" => paths,
        DataFiles::Splits(splits) => splits
            .into_iter()
            .filter(|s| s.split == selection.split)
            .flat_map(|s| match s.path {
                Paths::One(p) => vec![p],
                Paths::Many(p) => p,
            })
            .collect(),
        _ => Vec::new(),
    };
    if patterns.is_empty() {
        return Err(Error::new(
            ErrorKind::NotFound,
            "resolve_huggingface",
            "split has no declared files",
        ));
    }
    let mut files: Vec<_> = info.siblings.iter().map(|f| f.rfilename.as_str()).collect();
    files.sort_unstable();
    let mut seen = HashSet::new();
    let mut objects = Vec::new();
    let options = MatchOptions {
        case_sensitive: true,
        require_literal_separator: true,
        require_literal_leading_dot: true,
    };
    for pattern in patterns {
        if pattern.contains("://") {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "resolve_huggingface",
                "external data_files URLs are unsupported",
            ));
        }
        validate_path(&pattern, false)?;
        let path = if config.data_dir.is_empty() {
            pattern
        } else {
            format!("{}/{}", config.data_dir.trim_end_matches('/'), pattern)
        };
        let pattern = Pattern::new(&path).map_err(|e| {
            Error::new(
                ErrorKind::Decode,
                "resolve_huggingface",
                "invalid data_files glob",
            )
            .with_source(e)
        })?;
        let mut matched = false;
        for file in &files {
            if pattern.matches_with(file, options) {
                validate_path(file, false)?;
                matched = true;
                if seen.insert(*file) {
                    objects.push(ObjectRef::new(*file));
                }
            }
        }
        if !matched {
            return Err(Error::new(
                ErrorKind::NotFound,
                "resolve_huggingface",
                "a declared data_files pattern matched no repository files",
            ));
        }
    }
    Ok(objects)
}
