use std::{
    num::NonZeroUsize,
    sync::{Arc, OnceLock},
};

use arrow_schema::{DataType, Field, Schema};
use futures::{TryStreamExt, stream};
use open_arrow_access_core::{
    Accessor, ByteReadOptions, ByteReadRequest, ByteReader, Bytes, Capability, ObjectRef,
    ReadOptions, ReadOptionsProvider, RecordBatchBuilder, RecordMetadata, RecordReader, Result,
    ScanPartition, ScanPlan, ScanPlanner, SchemaRef, StreamReader, async_trait, into_stream,
    read_all,
};

use super::{ArxivKind, ArxivService};

const DEFAULT_BATCH_TARGET_BYTES: usize = 4 * 1024 * 1024;

/// Selection of independent TAR shards; entries inside each shard remain sequential.
#[derive(Clone, Debug)]
pub struct ArxivReadRequest {
    pub kind: ArxivKind,
    pub month: Option<u16>,
    pub options: ReadOptions,
    pub byte_options: ByteReadOptions,
    /// Soft serialized payload target per Arrow batch. A complete entry can
    /// exceed this target; batch_size still limits the number of rows.
    pub target_bytes: NonZeroUsize,
}

impl ArxivReadRequest {
    pub fn new(kind: ArxivKind) -> Self {
        Self {
            kind,
            month: None,
            options: ReadOptions::default(),
            byte_options: ByteReadOptions::default(),
            target_bytes: NonZeroUsize::new(DEFAULT_BATCH_TARGET_BYTES).unwrap(),
        }
    }
    pub fn with_month(mut self, month: u16) -> Self {
        self.month = Some(month);
        self
    }
    pub fn with_options(mut self, options: ReadOptions) -> Self {
        self.options = options;
        self
    }
    pub fn with_target_bytes(mut self, target_bytes: NonZeroUsize) -> Self {
        self.target_bytes = target_bytes;
        self
    }
    pub fn with_byte_options(mut self, options: ByteReadOptions) -> Self {
        self.byte_options = options;
        self
    }
}

impl ReadOptionsProvider for ArxivReadRequest {
    fn read_options(&self) -> &ReadOptions {
        &self.options
    }
    fn read_options_mut(&mut self) -> &mut ReadOptions {
        &mut self.options
    }
}

/// Native Arrow rows for regular TAR entries. Names and embedded PDF/source bytes
/// are retained as LargeBinary; no document extraction or external URL fetch occurs.
/// Each parallel partition owns one TAR stream. TarLimits still bound entry sizes;
/// A soft payload target avoids accumulating many large entries in one batch;
/// individual entries remain intact and may exceed that target.
pub struct ArxivRecords<A> {
    service: Arc<ArxivService<A>>,
}

impl<A> ArxivService<A> {
    pub fn records(self) -> ArxivRecords<A> {
        ArxivRecords {
            service: Arc::new(self),
        }
    }
}

pub fn record_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("shard_uri", DataType::LargeUtf8, false),
                Field::new("entry_index", DataType::UInt64, false),
                Field::new("entry_name", DataType::LargeBinary, false),
                Field::new("data", DataType::LargeBinary, false),
            ]))
        })
        .clone()
}

#[async_trait]
impl<A> Accessor for ArxivRecords<A>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static,
{
    type Request = ArxivReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> Capability {
        Capability {
            read: self.service.storage.capability().read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    async fn open(&self, request: ArxivReadRequest) -> Result<Box<dyn RecordReader>> {
        let plan = self.plan_scan(request).await?;
        let batches = plan
            .partitions
            .and_then(|partition| async { partition.await.map(into_stream) })
            .try_flatten();
        Ok(Box::new(StreamReader::new(batches, plan.metadata)))
    }
}

#[async_trait]
impl<A> ScanPlanner for ArxivRecords<A>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static,
{
    async fn plan_scan(&self, request: ArxivReadRequest) -> Result<ScanPlan> {
        let (schema, _) = request.options.resolve_projection(&record_schema())?;
        let archives = self
            .service
            .manifest(request.kind, request.month, request.byte_options)
            .await?;
        let metadata = RecordMetadata {
            schema: schema.clone(),
            source: None,
        };
        let partitions = archives
            .into_iter()
            .map(|archive| {
                let service = self.service.clone();
                let request = request.clone();
                let schema = schema.clone();
                Box::pin(async move {
                    let tar = service.stream(&archive, request.byte_options).await?;
                    let source = archive.filename;
                    let output = RecordMetadata {
                        schema,
                        source: Some(ObjectRef::new(source.clone())),
                    };
                    let batches = stream::try_unfold(tar, move |mut tar| {
                        let source = source.clone();
                        let projection = request.options.projection.clone();
                        let rows = request.options.batch_size.get();
                        let target_bytes = request.target_bytes.get() as u64;
                        async move {
                            let mut builder = RecordBatchBuilder::new(record_schema(), projection)?;
                            let mut payload_bytes = 0_u64;
                            while builder.len() < rows
                                && (builder.is_empty() || payload_bytes < target_bytes)
                            {
                                let Some((entry, mut body)) = tar.next_entry().await? else {
                                    break;
                                };
                                if !entry.is_file {
                                    drop(body);
                                    continue;
                                }
                                payload_bytes = payload_bytes.saturating_add(entry.size);
                                let data = if builder.schema().column_with_name("data").is_some() {
                                    Bytes::from(read_all(body, "read_arxiv_entry").await?)
                                } else {
                                    while body.next().await?.is_some() {}
                                    Bytes::new()
                                };
                                builder.push(|row| {
                                    row.append("shard_uri", source.as_str())?;
                                    row.append("entry_index", entry.index as u64)?;
                                    row.append("entry_name", entry.name.as_slice())?;
                                    row.append("data", data.as_ref())
                                })?;
                            }
                            if builder.is_empty() {
                                return Ok(None);
                            }
                            Ok(Some((builder.finish()?, tar)))
                        }
                    });
                    Ok(Box::new(StreamReader::new(batches, output)) as Box<dyn RecordReader>)
                }) as ScanPartition
            })
            .collect();
        Ok(ScanPlan::new(metadata, partitions))
    }
}
