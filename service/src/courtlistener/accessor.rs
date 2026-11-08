use std::sync::Arc;

use open_arrow_access_core::{
    Accessor, AccessorExt, ByteAccessor, ByteReadRequest, ByteReader, Capability, ListRequest,
    Result, async_trait, list_files,
};
use open_arrow_access_decompression::{Bzip2, DecompressionLayer};

use super::{CourtListenerSnapshot, snapshot::validate_table};

/// Discover bulk exports and read their original CSV bytes through Bzip2.
/// The input must expose compressed objects beneath `bulk-data/`.
/// CSV schema, null rules and Arrow conversion belong to the consumer.
pub struct CourtListenerBulkAccessor<A> {
    inner: Arc<A>,
}

impl<A> Clone for CourtListenerBulkAccessor<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<A> CourtListenerBulkAccessor<A> {
    pub fn new(inner: A) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl<A: ByteAccessor + 'static> CourtListenerBulkAccessor<A> {
    /// Discover one table's available exports, sorted by date with duplicates
    /// removed. Collects only matching object names, never table contents.
    /// Requires listing; explicit snapshot reads do not.
    pub async fn snapshots(&self, table: &str) -> Result<Vec<CourtListenerSnapshot>> {
        validate_table(table)?;
        let objects = list_files(
            self.inner.as_ref(),
            ListRequest::new("bulk-data/").with_recursive(false),
            |name| {
                Ok(
                    CourtListenerSnapshot::from_object(&format!("bulk-data/{name}").into())
                        .is_ok_and(|snapshot| snapshot.table() == table),
                )
            },
        )
        .await?;
        objects
            .iter()
            .map(CourtListenerSnapshot::from_object)
            .collect()
    }
}

#[async_trait]
impl<A: ByteAccessor + 'static> Accessor for CourtListenerBulkAccessor<A> {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        Capability {
            read: self.inner.capability().read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        CourtListenerSnapshot::from_object(&request.object)?;
        self.inner
            .clone()
            .layer(DecompressionLayer::new(Bzip2))
            .open(request)
            .await
    }
}

#[async_trait]
impl<A: ByteAccessor + 'static> ByteAccessor for CourtListenerBulkAccessor<A> {}
