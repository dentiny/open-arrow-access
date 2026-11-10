use open_arrow_access_core::Result;
use opendal::OperationContext;

use super::{HuggingFaceAccessor, HuggingFaceOptions, HuggingFaceRepository, validation};

/// Provider entry point for repository access and dataset selection.
#[derive(Clone, Debug)]
pub struct HuggingFaceService {
    pub(super) options: HuggingFaceOptions,
    pub(super) context: OperationContext,
}

impl HuggingFaceService {
    /// Resolve connection settings and load cached credentials asynchronously.
    /// Does not contact the Hub until discovery or file reading is requested.
    pub async fn new(mut options: HuggingFaceOptions) -> Result<Self> {
        options.resolve_environment().await;
        validation::validate_options(&options)?;
        let client = crate::http_client::HttpClient::new()?;
        Ok(Self {
            options,
            context: client.context,
        })
    }

    /// Access explicit files in a dataset repository. A floating revision is
    /// read as supplied.
    /// `dataset` pins the Parquet export automatically.
    pub async fn repository(
        &self,
        repository: HuggingFaceRepository,
    ) -> Result<HuggingFaceAccessor> {
        validation::validate_repository(&repository)?;
        let mut builder = opendal::services::Hf::default()
            .repo_type("dataset")
            .repo_id(&repository.repository)
            .revision(&repository.revision)
            .root(&repository.root)
            .endpoint(self.options.endpoint());
        if let Some(token) = &self.options.token {
            builder = builder.token(token);
        }
        HuggingFaceAccessor::new(builder, self.context.clone())
    }
}
