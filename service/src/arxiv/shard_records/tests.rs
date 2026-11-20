use super::*;
use crate::arxiv::{ArxivContentFormat, ArxivKind};
use open_arrow_access_core::{AccessorExt, ByteMetadata, Bytes, Capability, Reader, async_trait};
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_test_support::{MemoryAccessor, fixture};
use std::{num::NonZeroUsize, time::Duration};
use tokio::sync::Barrier;

struct GatedSource {
    source: MemoryAccessor,
    gate: Arc<Barrier>,
}
#[async_trait]
impl Accessor for GatedSource {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;
    fn capability(&self) -> Capability {
        self.source.capability()
    }
    async fn open(&self, request: Self::Request) -> Result<Box<dyn ByteReader>> {
        Ok(Box::new(GatedReader {
            reader: self.source.open(request).await?,
            gate: self.gate.clone(),
        }))
    }
}
struct GatedReader {
    reader: Box<dyn ByteReader>,
    gate: Arc<Barrier>,
}
#[async_trait]
impl Reader for GatedReader {
    type Item = Bytes;
    type Metadata = ByteMetadata;
    type Capability = Capability;
    fn capability(&self) -> Capability {
        self.reader.capability()
    }
    fn metadata(&self) -> &ByteMetadata {
        self.reader.metadata()
    }
    async fn next(&mut self) -> Result<Option<Bytes>> {
        self.reader.next().await
    }
}
#[async_trait]
impl ByteReader for GatedReader {
    async fn read_range(&mut self, range: Range<u64>) -> Result<Bytes> {
        if !range.is_empty() && range.end - range.start != 512 {
            // Neither article can finish reading until both reads are active.
            self.gate.wait().await;
        }
        self.reader.read_range(range).await
    }
}

#[tokio::test]
async fn a_single_shard_reads_members_concurrently_before_latex_parsing() {
    let bytes = fixture!("arxiv-content", "latex-two.tar").await;
    let source = MemoryAccessor::new(&bytes, 512).with_ranges();
    let observed = source.observations.clone();
    let records = ArxivService::new(GatedSource {
        source,
        gate: Arc::new(Barrier::new(2)),
    })
    .records()
    .content(ArxivContentFormat::Latex)
    .layer(ParallelReadLayer::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::MIN,
    ));
    let mut reader = records
        .open(ArxivReadRequest::new(ArxivKind::Source).with_shard("src/one.tar"))
        .await
        .unwrap();
    let mut rows = 0;
    while let Some(batch) = tokio::time::timeout(Duration::from_secs(2), reader.next())
        .await
        .unwrap()
        .unwrap()
    {
        assert_eq!(batch.schema(), crate::arxiv::content_schema());
        rows += batch.num_rows();
    }
    assert_eq!(rows, 2);
    let requests = observed.requests.lock().unwrap();
    assert_eq!(requests.len(), 3); // One index reader and two independent member readers.
    assert!(
        requests
            .iter()
            .all(|request| request.object.locator() == "src/one.tar")
    );
    assert!(
        requests[1..]
            .iter()
            .all(|request| request.content_length == Some(bytes.len() as u64))
    );
}
