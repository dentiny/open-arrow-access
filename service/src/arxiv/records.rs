use std::{
    num::NonZeroUsize,
    sync::{Arc, OnceLock},
};

use arrow_schema::{DataType, Field, Schema};
use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteReadOptions, ByteReadRequest, ByteReader, Capability, ObjectRef,
    OrderRequirement, ReadOptions, ReadOptionsProvider, RecordCapability, RecordMetadata,
    RecordReader, Result, ScanPlan, ScanPlanner, SchemaRef, StreamReader, async_trait,
    scan_partition,
};

use super::{ArxivKind, ArxivService, tar_records::read_tar_batch};

const DEFAULT_BATCH_TARGET_BYTES: usize = 4 * 1024 * 1024;

/// Selection of independent TAR shards; entries inside each shard remain sequential.
#[derive(Clone, Debug)]
pub struct ArxivReadRequest {
    /// Distribution selection; defaults to automatic discovery of both manifests.
    pub kind: ArxivKind,
    pub month: Option<u16>,
    /// One uncompressed TAR object, relative to the service storage root.
    /// Bypasses manifest discovery; cannot be combined with a month selector.
    pub shard: Option<String>,
    pub options: ReadOptions,
    pub byte_options: ByteReadOptions,
    /// Soft serialized payload target per Arrow batch. A complete entry can
    /// exceed this target; batch_size still limits the number of rows.
    pub target_bytes: NonZeroUsize,
}

impl Default for ArxivReadRequest {
    fn default() -> Self {
        Self::new(ArxivKind::Auto)
    }
}

impl ArxivReadRequest {
    pub fn new(kind: ArxivKind) -> Self {
        Self {
            kind,
            month: None,
            shard: None,
            options: ReadOptions::default(),
            byte_options: ByteReadOptions::default(),
            target_bytes: NonZeroUsize::new(DEFAULT_BATCH_TARGET_BYTES).unwrap(),
        }
    }
    pub fn with_month(mut self, month: u16) -> Self {
        self.month = Some(month);
        self
    }
    pub fn with_shard(mut self, shard: impl Into<String>) -> Self {
        self.shard = Some(shard.into());
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

    fn byte_read_options_mut(&mut self) -> Option<&mut ByteReadOptions> {
        Some(&mut self.byte_options)
    }
}

/// Native Arrow rows for regular TAR entries, preserving names and payloads as LargeBinary.
/// Each partition owns one sequential TAR stream; TarLimits apply.
/// See [`ArxivReadRequest::target_bytes`] for batch payload sizing.
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

    fn capability(&self) -> RecordCapability {
        Capability {
            read: self.service.storage.capability().read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
        .with_ordering(OrderRequirement::Reorderable)
    }

    async fn open(&self, request: ArxivReadRequest) -> Result<Box<dyn RecordReader>> {
        Ok(Box::new(self.plan_scan(request).await?.into_reader()))
    }
}

#[async_trait]
impl<A> ScanPlanner for ArxivRecords<A>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static,
{
    async fn plan_scan(&self, request: ArxivReadRequest) -> Result<ScanPlan> {
        let schema = request.options.resolve_projection(&record_schema())?.schema;
        let archives = if let Some(shard) = &request.shard {
            if request.month.is_some()
                || !shard.ends_with(".tar")
                || shard.starts_with('/')
                || shard.contains(['\\', ':', '?', '#'])
                || shard.split('/').any(|part| matches!(part, "" | "." | ".."))
            {
                return Err(super::error::invalid(
                    "shard must be a relative TAR key without a month selector",
                ));
            }
            vec![(shard.clone(), None)]
        } else {
            self.service
                .manifest(request.kind, request.month, request.byte_options)
                .await?
                .into_iter()
                .map(|archive| (archive.filename.clone(), Some(archive)))
                .collect()
        };
        let metadata = RecordMetadata {
            schema: schema.clone(),
            source: None,
        };
        let service = self.service.clone();
        let partitions = archives.into_iter().map(move |(source, archive)| {
            let service = service.clone();
            let request = request.clone();
            let schema = schema.clone();
            scan_partition(async move {
                let tar = if let Some(archive) = archive {
                    service.stream(&archive, request.byte_options).await?
                } else {
                    let input = service
                        .storage
                        .open(
                            ByteReadRequest::new(source.clone()).with_options(request.byte_options),
                        )
                        .await?;
                    super::TarStream::new(input, service.tar_limits.clone())
                };
                let output = RecordMetadata {
                    schema,
                    source: Some(ObjectRef::new(source.clone())),
                };
                let batches = stream::try_unfold(tar, move |tar| {
                    let source = source.clone();
                    read_tar_batch(
                        tar,
                        record_schema(),
                        request.options.clone(),
                        request.target_bytes.get(),
                        move |row, entry, data| {
                            row.append("shard_uri", source.as_str())?;
                            row.append("entry_index", entry.index as u64)?;
                            row.append("entry_name", entry.name.as_slice())?;
                            row.append("data", data)
                        },
                    )
                });
                Ok(StreamReader::records(
                    batches,
                    output,
                    OrderRequirement::Reorderable,
                ))
            })
        });
        Ok(ScanPlan::from_iter(metadata, partitions).with_ordering(self.capability().ordering))
    }
}
