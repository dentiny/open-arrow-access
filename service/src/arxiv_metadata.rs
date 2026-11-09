//! ArxivMetadata metadata harvesting via the shared OAI-PMH protocol.
use crate::oai::{OaiProvider, OaiProviderService};
#[derive(Clone, Debug)]
pub struct ArxivMetadata;
impl OaiProvider for ArxivMetadata {
    const ORIGIN: &'static str = "https://oaipmh.arxiv.org";
    const PATH: &'static str = "oai";
    const METADATA_PREFIX: &'static str = "arXivRaw";
}
pub type ArxivMetadataService = OaiProviderService<ArxivMetadata>;
