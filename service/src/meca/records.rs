use std::{
    num::NonZeroUsize,
    sync::{Arc, OnceLock},
};

use arrow_schema::{DataType, Field, Schema};
use futures::{TryStreamExt, lock::Mutex, stream};
use open_arrow_access_core::{
    Accessor, Bytes, Capability, ReadOptions, ReadOptionsProvider, RecordBatchBuilder,
    RecordMetadata, RecordReader, Result, ScanPartition, ScanPlan, ScanPlanner, SchemaRef,
    StreamReader, async_trait, into_stream, read_all,
};

use super::MecaArchive;

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

/// One manifest instance per Arrow batch, in manifest order, with original payload bytes.
/// Entry readers share the ZIP source and obey ZipLimits; parallel decoding
/// does not imply concurrent underlying range I/O.
pub struct MecaRecords {
    archive: Arc<Mutex<MecaArchive>>,
}

impl MecaArchive {
    pub fn records(self) -> MecaRecords {
        MecaRecords {
            archive: Arc::new(Mutex::new(self)),
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
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }
    async fn open(&self, request: MecaReadRequest) -> Result<Box<dyn RecordReader>> {
        let plan = self.plan_scan(request).await?;
        let batches = plan
            .partitions
            .and_then(|partition| async { partition.await.map(into_stream) })
            .try_flatten();
        Ok(Box::new(StreamReader::new(batches, plan.metadata)))
    }
}

#[async_trait]
impl ScanPlanner for MecaRecords {
    async fn plan_scan(&self, request: MecaReadRequest) -> Result<ScanPlan> {
        let (schema, _) = request.options.resolve_projection(&record_schema())?;
        let metadata = RecordMetadata {
            schema: schema.clone(),
            source: None,
        };
        let session = self.archive.lock().await;
        let files = session.files.clone();
        let archive = session.archive.clone();
        drop(session);
        let partitions = files
            .into_iter()
            .map(|file| {
                let archive = archive.clone();
                let request = request.clone();
                let schema = schema.clone();
                Box::pin(async move {
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
                        let data = if builder.schema().column_with_name("data").is_some() {
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
                    Ok(Box::new(StreamReader::new(batches, metadata)) as Box<dyn RecordReader>)
                }) as ScanPartition
            })
            .collect();
        Ok(ScanPlan::new(metadata, partitions))
    }
}
