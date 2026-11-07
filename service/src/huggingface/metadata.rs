use http::{Request, StatusCode};
use open_arrow_access_core::{Error, ErrorKind, Result};
use opendal::{Buffer, raw::oio::ReadStream};
use serde::Deserialize;

use super::{HuggingFaceRepository, HuggingFaceService, client::endpoint, map_error};

const MAX_METADATA_BYTES: usize = 64 * 1024;

#[derive(Deserialize)]
struct RepositoryInfo {
    sha: String,
}

impl HuggingFaceService {
    pub(super) async fn pin_revision(
        &self,
        mut repository: HuggingFaceRepository,
    ) -> Result<HuggingFaceRepository> {
        if is_commit_sha(&repository.revision) {
            return Ok(repository);
        }
        repository.revision = self.info(&repository).await?.sha;
        Ok(repository)
    }

    async fn info(&self, repository: &HuggingFaceRepository) -> Result<RepositoryInfo> {
        let mut url = endpoint(self.options.endpoint())?;
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
        url.query_pairs_mut().append_pair("expand", "sha");
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
                    "revision metadata exceeds the 64 KiB limit",
                ));
            }
            for part in buffer {
                bytes.extend_from_slice(&part);
            }
        }
        let metadata: RepositoryInfo = serde_json::from_slice(&bytes).map_err(|error| {
            Error::new(
                ErrorKind::Decode,
                "resolve_huggingface",
                "invalid revision metadata",
            )
            .with_source(error)
        })?;
        if !is_commit_sha(&metadata.sha) {
            return Err(Error::new(
                ErrorKind::Decode,
                "resolve_huggingface",
                "revision metadata did not contain a commit SHA",
            ));
        }
        Ok(metadata)
    }
}

fn is_commit_sha(revision: &str) -> bool {
    revision.len() == 40 && revision.bytes().all(|b| b.is_ascii_hexdigit())
}
