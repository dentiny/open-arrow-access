use super::{OaiPage, OaiService};
use crate::common::Connection;
use open_arrow_access_core::Result;

/// Provider-owned endpoint configuration, shared by otherwise identical harvesters.
pub trait OaiProvider: Clone + std::fmt::Debug + Send + Sync {
    const ORIGIN: &'static str;
    const PATH: &'static str;
    const METADATA_PREFIX: &'static str;
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
