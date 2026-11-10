//! HathiTrust's published full and incremental catalogue files.
//!
//! Resolved files expose `records(format)` for explicit Arrow decoding and scan
//! planning. The adapter retains catalogue filenames and declared file metadata;
//! metadata links are not followed automatically. API pages can bind an explicit
//! JSON record selection with `ApiDocument::records_at`.
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
        self.connection
            .resolve_json_files("files/hathifiles/hathi_file_list.json", &[], |doc| {
                resolver::resolve_catalog_files(&self.connection, doc.value())
            })
            .await
    }
}
