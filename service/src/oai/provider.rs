use super::{OaiPage, OaiService};
use crate::common::Connection;
use open_arrow_access_core::Result;
use open_arrow_access_file_format::xml::XmlFormat;

/// Provider-owned endpoint configuration and default record mapping.
pub trait OaiProvider: Clone + std::fmt::Debug + Send + Sync {
    const ORIGIN: &'static str;
    const PATH: &'static str;
    const METADATA_PREFIX: &'static str;

    /// Decode standard OAI-PMH headers with a fixed schema, including empty pages.
    ///
    /// Columns are `identifier`, `datestamp`, `status`, and `set_specs`. The OAI
    /// namespace is matched by URI, independently of input prefixes. Provider
    /// metadata is kept in the original page bytes; use an explicit XML mapping
    /// to select it, or override this method with a provider-specific format.
    fn record_format() -> XmlFormat {
        super::record_format::default_record_format()
    }

    /// Full record mapping. Providers with a published schema can bind known
    /// types instead of guessing identifier/scalar types from sampled values.
    fn inferred_record_format() -> XmlFormat {
        super::record_format::inferred_record_format()
    }
}
/// Configured OAI-PMH service; marker types add no runtime state.
#[derive(Clone, Debug)]
pub struct OaiProviderService<P> {
    protocol: OaiService,
    provider: std::marker::PhantomData<P>,
}
impl<P: OaiProvider> OaiProviderService<P> {
    pub fn new() -> Result<Self> {
        Self::with_connection(Connection::new(P::ORIGIN)?)
    }

    pub fn with_connection(connection: Connection) -> Result<Self> {
        Ok(Self {
            protocol: OaiService::new(connection, P::PATH, P::METADATA_PREFIX)?,
            provider: std::marker::PhantomData,
        })
    }

    pub fn connection(&self) -> &Connection {
        self.protocol.connection()
    }

    /// The provider's default Arrow record mapping for `page.records(format)`.
    ///
    /// Providers normally use standard OAI-PMH header columns. arXiv overrides
    /// this with its complete arXivRaw record, including license and history.
    /// Other providers can override `OaiProvider::record_format` as needed.
    pub fn record_format(&self) -> XmlFormat {
        P::record_format()
    }

    /// Infer all fields beneath each standard OAI-PMH record, including provider
    /// metadata, using the provider's mapping when available, otherwise the
    /// shared record path and namespace binding with sampled inference.
    ///
    /// Unlike [`Self::record_format`], this does not fix the output to protocol
    /// header columns. The default schema depends on sampled page contents;
    /// providers such as arXiv bind a fixed full schema instead. Use an explicit
    /// XML schema when other providers need stable mappings across pages.
    /// This is independent of a custom [`OaiProvider::record_format`] override.
    pub fn inferred_record_format(&self) -> XmlFormat {
        P::inferred_record_format()
    }

    /// Scan all continuation pages using the provider's default record mapping.
    /// Use `with_format` on the harvester for explicit metadata field mappings.
    pub fn harvest(&self) -> super::OaiHarvester {
        self.protocol.harvest(self.record_format())
    }

    pub async fn records(
        &self,
        from: Option<&str>,
        until: Option<&str>,
        set: Option<&str>,
    ) -> Result<OaiPage> {
        self.protocol.records(from, until, set).await
    }

    pub async fn resume(&self, token: &str) -> Result<OaiPage> {
        self.protocol.resume(token).await
    }
}
