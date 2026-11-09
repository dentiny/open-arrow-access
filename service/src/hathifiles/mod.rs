//! HathiTrust's published full and incremental catalogue files.
use crate::common::{Connection, RemoteFiles};
mod error;
mod resolver;
use open_arrow_access_core::Result;
#[derive(Clone, Debug)]
pub struct HathifilesService {
    connection: Connection,
}
impl HathifilesService {
    pub fn new() -> Result<Self> {
        Ok(Self::with_connection(Connection::new(
            "https://www.hathitrust.org",
        )?))
    }

    pub fn with_connection(connection: Connection) -> Self {
        Self { connection }
    }

    /// Resolve the upstream ordered catalogue. No full/update or rights filtering is applied.
    pub async fn catalog(&self) -> Result<RemoteFiles> {
        let doc = self
            .connection
            .json("files/hathifiles/hathi_file_list.json", &[])
            .await?;
        let files = resolver::resolve_catalog_files(&self.connection, doc.value())?;
        self.connection.files(files, doc)
    }
}
