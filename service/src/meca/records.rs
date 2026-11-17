use std::{
    num::NonZeroUsize,
    sync::{Arc, OnceLock},
};

use arrow_schema::{DataType, Field, Schema};
use futures::{lock::Mutex, stream};
use open_arrow_access_core::{
    Accessor, Bytes, Capability, FieldNameIndex, OrderRequirement, ReadOptions,
    ReadOptionsProvider, RecordBatchBuilder, RecordCapability, RecordMetadata, RecordReader,
    Result, ScanPlan, ScanPlanner, SchemaRef, StreamReader, async_trait, read_all, scan_partition,
};

use open_arrow_access_decompression::zip::ZipArchive;

use super::{MecaArchive, MecaFile};

const DEFAULT_CHUNK_SIZE: NonZeroUsize = NonZeroUsize::new(64 * 1024).unwrap();

/// Read settings for a previously opened and validated MECA archive.
#[derive(Clone, Debug)]
pub struct MecaReadRequest {
    pub options: ReadOptions,
    /// Maximum decoded bytes per entry-reader chunk. Source I/O options are fixed
    /// by the ByteReader supplied when opening the archive.
    pub chunk_size: NonZeroUsize,
}

impl MecaReadRequest {
    pub fn with_options(mut self, options: ReadOptions) -> Self {
        self.options = options;
        self
    }
    pub fn with_chunk_size(mut self, chunk_size: NonZeroUsize) -> Self {
        self.chunk_size = chunk_size;
        self
    }
}

impl Default for MecaReadRequest {
    fn default() -> Self {
        Self {
            options: ReadOptions::default(),
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }
}

impl ReadOptionsProvider for MecaReadRequest {
    fn read_options(&self) -> &ReadOptions {
        &self.options
    }
    fn read_options_mut(&mut self) -> &mut ReadOptions {
        &mut self.options
    }
}

/// One Arrow row and batch per manifest instance, in manifest order, with unchanged payloads.
/// Entry readers share the ZIP source without implying concurrent range I/O.
/// ZipLimits bound entry decoding.
pub struct MecaRecords {
    archive: Mutex<ZipArchive>,
    files: Arc<[MecaFile]>,
}

impl MecaArchive {
    pub fn records(self) -> MecaRecords {
        MecaRecords {
            archive: Mutex::new(self.archive),
            files: self.files.into(),
        }
    }
}

pub fn record_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("item_id", DataType::Utf8, false),
                Field::new("item_type", DataType::Utf8, true),
                Field::new("href", DataType::LargeUtf8, false),
                Field::new("media_type", DataType::Utf8, true),
                Field::new("entry_index", DataType::UInt64, false),
                Field::new("data", DataType::LargeBinary, false),
            ]))
        })
        .clone()
}

#[async_trait]
impl Accessor for MecaRecords {
    type Request = MecaReadRequest;
    type Reader = dyn RecordReader;
    fn capability(&self) -> RecordCapability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
        .with_ordering(OrderRequirement::Reorderable)
    }
    async fn open(&self, request: MecaReadRequest) -> Result<Box<dyn RecordReader>> {
        Ok(Box::new(self.plan_scan(request).await?.into_reader()))
    }
}

#[async_trait]
impl ScanPlanner for MecaRecords {
    async fn plan_scan(&self, request: MecaReadRequest) -> Result<ScanPlan> {
        let schema = request.options.resolve_projection(&record_schema())?.schema;
        let include_data = FieldNameIndex::new(schema.fields()).get("data").is_some();
        let metadata = RecordMetadata {
            schema: schema.clone(),
            source: None,
        };
        let archive = self.archive.lock().await.clone();
        let files = self.files.clone();
        let partitions = (0..files.len()).map(move |index| {
            let file = files[index].clone();
            let archive = archive.clone();
            let request = request.clone();
            let schema = schema.clone();
            scan_partition(async move {
                let metadata = RecordMetadata {
                    schema,
                    source: None,
                };
                let batches = stream::once(async move {
                    let mut body = archive
                        .open_entry(file.entry_index, request.chunk_size)
                        .await?;
                    let mut builder =
                        RecordBatchBuilder::new(record_schema(), request.options.projection)?;
                    let data = if include_data {
                        Bytes::from(read_all(body, "read_meca_entry").await?)
                    } else {
                        while body.next().await?.is_some() {}
                        Bytes::new()
                    };
                    builder.push(|row| {
                        row.append("item_id", file.item_id.as_str())?;
                        row.append("item_type", file.item_type.as_deref())?;
                        row.append("href", file.href.as_str())?;
                        row.append("media_type", file.media_type.as_deref())?;
                        row.append("entry_index", file.entry_index as u64)?;
                        row.append("data", data.as_ref())
                    })?;
                    builder.finish()
                });
                Ok(StreamReader::records(
                    batches,
                    metadata,
                    OrderRequirement::Reorderable,
                ))
            })
        });
        Ok(ScanPlan::from_iter(metadata, partitions).with_ordering(self.capability().ordering))
    }
}
