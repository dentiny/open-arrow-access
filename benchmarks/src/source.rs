use open_arrow_access_core::{
    Accessor, ByteMetadata, ByteReadRequest, ByteReader, Bytes, Capability, Reader, Result,
    async_trait,
};
use open_arrow_access_storage::OpenDalAccessor;
use opendal::Operator;
use serde_json::json;
use std::{
    num::NonZeroUsize,
    ops::Range,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

#[derive(Default)]
struct Counters {
    calls: AtomicU64,
    ranges: AtomicU64,
    bytes: AtomicU64,
}

#[derive(Clone)]
pub struct Source {
    inner: Option<OpenDalAccessor>,
    raw: Bytes,
    chunk: usize,
    counters: Arc<Counters>,
}

impl Source {
    pub async fn new(
        root: &Path,
        backend: &str,
        name: &str,
        chunk: NonZeroUsize,
    ) -> super::AnyResult<Self> {
        opendal::install_default();
        let mut raw = Bytes::new();
        let operator = match backend {
            "raw" => {
                raw = std::fs::read(root.join(name))?.into();
                None
            }
            "fs" => Some(Operator::new(
                opendal::services::Fs::default().root(root.to_str().ok_or("non UTF-8 root")?),
            )?),
            "memory" => {
                let op = Operator::new(opendal::services::Memory::default())?;
                op.write(name, std::fs::read(root.join(name))?).await?;
                Some(op)
            }
            "http" => Some(Operator::new(
                opendal::services::Http::default()
                    .endpoint(&std::env::var("OAA_BENCH_HTTP_ENDPOINT")?),
            )?),
            // Explicit configuration only; no discovery, listing or writes to cloud storage.
            "s3" => {
                let config: std::collections::HashMap<String, String> =
                    serde_json::from_str(&std::env::var("OAA_BENCH_STORAGE_CONFIG")?)?;
                Some(Operator::via_iter("s3", config)?)
            }
            _ => return Err("unknown backend".into()),
        };
        Ok(Self {
            inner: operator.map(|op| OpenDalAccessor::new(op).with_chunk_size(chunk)),
            raw,
            chunk: chunk.get(),
            counters: Arc::default(),
        })
    }

    pub fn stats(&self) -> serde_json::Value {
        json!({"next_calls":self.counters.calls.load(Ordering::Relaxed), "ranges":self.counters.ranges.load(Ordering::Relaxed), "bytes":self.counters.bytes.load(Ordering::Relaxed)})
    }
}

#[async_trait]
impl Accessor for Source {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;
    fn capability(&self) -> Capability {
        self.inner.as_ref().map_or(
            Capability {
                read: true,
                read_with_range: true,
                read_parallel: false,
                list: false,
            },
            Accessor::capability,
        )
    }
    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        let inner: Box<dyn ByteReader> = if let Some(inner) = &self.inner {
            inner.open(request).await?
        } else {
            Box::new(Raw {
                bytes: self.raw.clone(),
                offset: 0,
                chunk: self.chunk,
                metadata: ByteMetadata {
                    source: Some(request.object),
                    content_length: Some(self.raw.len() as u64),
                    version: None,
                },
            })
        };
        Ok(Box::new(Counted {
            inner,
            counters: self.counters.clone(),
        }))
    }
}

struct Counted {
    inner: Box<dyn ByteReader>,
    counters: Arc<Counters>,
}
#[async_trait]
impl Reader for Counted {
    type Item = Bytes;
    type Metadata = ByteMetadata;
    fn capability(&self) -> Capability {
        self.inner.capability()
    }
    fn metadata(&self) -> &ByteMetadata {
        self.inner.metadata()
    }
    async fn next(&mut self) -> Result<Option<Bytes>> {
        self.counters.calls.fetch_add(1, Ordering::Relaxed);
        let bytes = self.inner.next().await?;
        if let Some(bytes) = &bytes {
            self.counters
                .bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        Ok(bytes)
    }
}
#[async_trait]
impl ByteReader for Counted {
    async fn read_range(&mut self, range: Range<u64>) -> Result<Bytes> {
        self.counters.ranges.fetch_add(1, Ordering::Relaxed);
        let bytes = self.inner.read_range(range).await?;
        self.counters
            .bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(bytes)
    }
    async fn read_ranges(&mut self, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        self.counters
            .ranges
            .fetch_add(ranges.len() as u64, Ordering::Relaxed);
        let bytes = self.inner.read_ranges(ranges).await?;
        self.counters.bytes.fetch_add(
            bytes.iter().map(|b| b.len() as u64).sum::<u64>(),
            Ordering::Relaxed,
        );
        Ok(bytes)
    }
}

struct Raw {
    bytes: Bytes,
    offset: usize,
    chunk: usize,
    metadata: ByteMetadata,
}
#[async_trait]
impl Reader for Raw {
    type Item = Bytes;
    type Metadata = ByteMetadata;
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: true,
            read_parallel: false,
            list: false,
        }
    }
    fn metadata(&self) -> &ByteMetadata {
        &self.metadata
    }
    async fn next(&mut self) -> Result<Option<Bytes>> {
        if self.offset == self.bytes.len() {
            return Ok(None);
        }
        let end = self.offset.saturating_add(self.chunk).min(self.bytes.len());
        let bytes = self.bytes.slice(self.offset..end);
        self.offset = end;
        Ok(Some(bytes))
    }
}
#[async_trait]
impl ByteReader for Raw {
    async fn read_range(&mut self, range: Range<u64>) -> Result<Bytes> {
        if range.start > range.end || range.end > self.bytes.len() as u64 {
            return Err(open_arrow_access_core::Error::new(
                open_arrow_access_core::ErrorKind::InvalidInput,
                "bench_range",
                "invalid range",
            ));
        }
        Ok(self.bytes.slice(range.start as usize..range.end as usize))
    }
}
