use open_arrow_access_core::{Error, ErrorKind, Result};
use opendal::OperationContext;
use url::Url;

use super::{
    HuggingFaceAccessor, HuggingFaceOptions, HuggingFaceRepository, validation::validate_repository,
};

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
        endpoint(options.endpoint())?;
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
        let client = crate::http_client::HttpClient::new();
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
        validate_repository(&repository)?;
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

pub(super) fn endpoint(value: &str) -> Result<Url> {
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
