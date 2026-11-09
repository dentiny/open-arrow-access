use super::error::decode;
use super::{ApiDocument, Connection, download_url};
use crate::ResolvedFileSet;
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, Result, async_trait,
};
use open_arrow_access_storage::NamedFileAccessor;

impl Connection {
    /// Bind a validated provider manifest to lazy byte reads. Logical names remain opaque.
    pub fn files<M>(&self, files: Vec<(String, String)>, metadata: M) -> Result<RemoteFiles<M>> {
        for (_, url) in &files {
            download_url(url)?;
        }
        let files = NamedFileAccessor::new(
            RemoteHttpAccessor(self.clone()),
            files
                .into_iter()
                .map(|(name, url)| (name, url.into()))
                .collect(),
        )
        .map_err(|e| decode("invalid file namespace").with_source(e))?;
        Ok(ResolvedFileSet::new(files, metadata))
    }
}

/// A manifest's provider metadata alongside a fixed namespace of sequential files.
pub type RemoteFiles<M = ApiDocument> = ResolvedFileSet<RemoteHttpAccessor, M>;
#[derive(Clone, Debug)]
pub struct RemoteHttpAccessor(Connection);
#[async_trait]
impl Accessor for RemoteHttpAccessor {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }
    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.0.open(request.object.locator(), request.options).await
    }
}
