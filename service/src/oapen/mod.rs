//! Oapen metadata harvesting via the shared OAI-PMH protocol.
use crate::oai::{OaiProvider, OaiProviderService};
#[derive(Clone, Debug)]
pub struct Oapen;
impl OaiProvider for Oapen {
    const ORIGIN: &'static str = "https://library.oapen.org";
    const PATH: &'static str = "oai/request";
    const METADATA_PREFIX: &'static str = "xoai";
}
pub type OapenService = OaiProviderService<Oapen>;

impl OaiProviderService<Oapen> {
    /// Read an OAPEN-hosted download link obtained from harvested metadata.
    /// The caller selects the representation; no publisher-site crawling is performed.
    pub async fn open_book(
        &self,
        url: &str,
        options: open_arrow_access_core::ByteReadOptions,
    ) -> open_arrow_access_core::Result<Box<dyn open_arrow_access_core::ByteReader>> {
        let link = crate::common::resolve_download_url(url)?;
        validation::validate_hosted_book(self.connection().origin(), &link)?;
        self.connection().open(url, options).await
    }
}

mod error;
mod validation;
