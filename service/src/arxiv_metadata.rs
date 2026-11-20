//! ArxivMetadata metadata harvesting via the shared OAI-PMH protocol.
mod format;

pub(crate) use format::schema;

use crate::oai::{OaiProvider, OaiProviderService};
#[derive(Clone, Debug)]
pub struct ArxivMetadata;
impl OaiProvider for ArxivMetadata {
    const ORIGIN: &'static str = "https://oaipmh.arxiv.org";
    const PATH: &'static str = "oai";
    const METADATA_PREFIX: &'static str = "arXivRaw";

    fn inferred_record_format() -> open_arrow_access_file_format::xml::XmlFormat {
        format::record_format()
    }
}
pub type ArxivMetadataService = OaiProviderService<ArxivMetadata>;
