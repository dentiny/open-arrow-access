use std::{fmt, path::PathBuf};

use open_arrow_access_core::{Error, ErrorKind, Result};

const DEFAULT_ENDPOINT: &str = "https://huggingface.co";

/// Connection settings shared by metadata discovery and repository file reads.
#[derive(Clone, Default)]
pub struct HuggingFaceOptions {
    endpoint: Option<String>,
    pub(super) token: Option<String>,
}

impl fmt::Debug for HuggingFaceOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HuggingFaceOptions")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl HuggingFaceOptions {
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    pub(super) fn endpoint(&self) -> &str {
        self.endpoint.as_deref().unwrap_or(DEFAULT_ENDPOINT)
    }

    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    // Match OpenDAL's HF credential precedence so
    // Hub metadata requests and file requests use the same effective identity.
    pub(super) async fn resolve_environment(&mut self) {
        self.endpoint.get_or_insert_with(|| {
            std::env::var("HF_ENDPOINT").unwrap_or_else(|_| DEFAULT_ENDPOINT.into())
        });
        if self.token.is_some()
            || std::env::var("HF_HUB_DISABLE_IMPLICIT_TOKEN").is_ok_and(|v| !v.is_empty())
        {
            return;
        }
        self.token = std::env::var("HF_TOKEN").ok().filter(|s| !s.is_empty());
        if self.token.is_none() {
            self.token = read_cached_token().await;
        }
    }
}

async fn read_cached_token() -> Option<String> {
    let path = if let Some(path) = std::env::var_os("HF_TOKEN_PATH") {
        PathBuf::from(path)
    } else {
        [
            ("HF_HOME", "token"),
            ("XDG_CACHE_HOME", "huggingface/token"),
            ("HOME", ".cache/huggingface/token"),
        ]
        .into_iter()
        .find_map(|(name, suffix)| {
            std::env::var_os(name).map(|path| PathBuf::from(path).join(suffix))
        })?
    };
    let token = tokio::fs::read_to_string(path).await.ok()?;
    let token = token.trim();
    (!token.is_empty()).then(|| token.to_owned())
}

/// A dataset repository and its file namespace. Revisions may be branches or SHAs.
#[derive(Clone, Debug)]
pub struct HuggingFaceRepository {
    pub repository: String,
    pub revision: String,
    pub root: String,
}

impl HuggingFaceRepository {
    pub fn new(repository: impl Into<String>) -> Self {
        Self {
            repository: repository.into(),
            revision: "main".into(),
            root: String::new(),
        }
    }

    pub fn with_revision(mut self, revision: impl Into<String>) -> Self {
        self.revision = revision.into();
        self
    }

    pub fn with_root(mut self, root: impl Into<String>) -> Self {
        self.root = root.into();
        self
    }

    pub(super) fn validate(&self) -> Result<()> {
        let parts: Vec<_> = self.repository.split('/').collect();
        if parts.len() != 2
            || parts.iter().any(|p| {
                p.is_empty()
                    || *p == "."
                    || *p == ".."
                    || !p
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
            })
        {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "configure_huggingface",
                "expected a repository ID in owner/name form",
            ));
        }
        if self.revision.is_empty()
            || self.revision.trim() != self.revision
            || self.revision.contains(['\0', '\\'])
            || matches!(self.revision.as_str(), "." | "..")
        {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "configure_huggingface",
                "invalid repository revision",
            ));
        }
        super::validation::validate_path(&self.root, true)
    }
}
