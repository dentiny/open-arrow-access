use std::{fmt, path::PathBuf};

use open_arrow_access_core::{Error, ErrorKind, Result};

/// Connection settings shared by metadata discovery and repository file reads.
#[derive(Clone)]
pub struct HuggingFaceOptions {
    pub(super) endpoint: String,
    endpoint_explicit: bool,
    pub(super) token: Option<String>,
    pub(super) http_only: bool,
}

impl Default for HuggingFaceOptions {
    fn default() -> Self {
        Self {
            endpoint: "https://huggingface.co".into(),
            endpoint_explicit: false,
            token: None,
            http_only: false,
        }
    }
}

impl fmt::Debug for HuggingFaceOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HuggingFaceOptions")
            .field("endpoint", &self.endpoint)
            .field("http_only", &self.http_only)
            .finish_non_exhaustive()
    }
}

impl HuggingFaceOptions {
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self.endpoint_explicit = true;
        self
    }

    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Use HTTP downloads instead of the upstream default HTTP/Xet selection.
    pub fn with_http_downloads(mut self) -> Self {
        self.http_only = true;
        self
    }

    // Called on the blocking pool. Match OpenDAL's HF credential precedence so
    // Hub metadata requests and file requests use the same effective identity.
    pub(super) fn resolve_environment(&mut self) {
        if !self.endpoint_explicit {
            self.endpoint = std::env::var("HF_ENDPOINT").unwrap_or_else(|_| self.endpoint.clone());
        }
        if self.token.is_some()
            || std::env::var("HF_HUB_DISABLE_IMPLICIT_TOKEN").is_ok_and(|v| !v.is_empty())
        {
            return;
        }
        self.token = std::env::var("HF_TOKEN").ok().filter(|s| !s.is_empty());
        if self.token.is_none() {
            let path = std::env::var_os("HF_TOKEN_PATH")
                .map(PathBuf::from)
                .or_else(|| {
                    std::env::var_os("HF_HOME")
                        .map(PathBuf::from)
                        .or_else(|| {
                            std::env::var_os("XDG_CACHE_HOME")
                                .map(|p| PathBuf::from(p).join("huggingface"))
                        })
                        .or_else(|| {
                            std::env::var_os("HOME")
                                .map(|p| PathBuf::from(p).join(".cache/huggingface"))
                        })
                        .map(|p| p.join("token"))
                });
            self.token = path
                .and_then(|p| std::fs::read_to_string(p).ok())
                .map(|s| s.trim().to_owned())
                .filter(|s| !s.is_empty());
        }
    }
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
        super::accessor::validate_path(&self.root, true)
    }
}
