use open_arrow_access_core::{Error, ErrorKind, Result};
use serde::Deserialize;

use crate::common::is_commit_sha;

use super::{
    HuggingFaceRepository, HuggingFaceService,
    validation::{endpoint, validate_commit_sha},
};

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
        let metadata: RepositoryInfo = crate::http_client::HttpClient {
            context: self.context.clone(),
        }
        .json(
            url.as_str(),
            self.options.token.as_deref(),
            "resolve_huggingface",
        )
        .await?;
        validate_commit_sha(&metadata.sha)?;
        Ok(metadata)
    }
}
