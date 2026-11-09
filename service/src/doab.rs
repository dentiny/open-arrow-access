//! Doab metadata harvesting via the shared OAI-PMH protocol.
use crate::oai::{OaiProvider, OaiProviderService};
#[derive(Clone, Debug)]
pub struct Doab;
impl OaiProvider for Doab {
    const ORIGIN: &'static str = "https://directory.doabooks.org";
    const PATH: &'static str = "oai/request";
    const METADATA_PREFIX: &'static str = "xoai";
}
pub type DoabService = OaiProviderService<Doab>;
