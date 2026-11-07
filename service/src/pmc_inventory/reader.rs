use super::{capability, decode, validation};
use arrow_array::{Array, ArrayRef, StringArray};
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteAccessor, ByteReadOptions, Capability, ObjectRef, ReadOptions,
    ReadRequest, Reader, RecordBatch, RecordMetadata, RecordReader, Result, SchemaRef, async_trait,
};
use open_arrow_access_decompression::{DecompressionLayer, Gzip};
use open_arrow_access_file_format::{CsvAccessor, CsvFormat};
use std::{collections::VecDeque, sync::Arc};

pub(super) struct InventoryReader<A> {
    pub source: Arc<A>,
    pub files: VecDeque<ObjectRef>,
    pub current: Option<Box<dyn RecordReader>>,
    pub csv_schema: SchemaRef,
    pub full_schema: SchemaRef,
    pub projection: Option<Vec<usize>>,
    pub options: ReadOptions,
    pub byte_options: ByteReadOptions,
    pub metadata: RecordMetadata,
    pub done: bool,
}
impl<A: ByteAccessor + 'static> InventoryReader<A> {
    async fn read_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if let Some(reader) = &mut self.current {
                if let Some(batch) = reader.next().await? {
                    let buckets = batch
                        .column_by_name("Bucket")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    let keys = batch
                        .column_by_name("Key")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    let decoded = (0..batch.num_rows())
                        .map(|row| {
                            validation::validate_row(
                                (!buckets.is_null(row)).then(|| buckets.value(row)),
                                (!keys.is_null(row)).then(|| keys.value(row)),
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let mut columns = batch.columns().to_vec();
                    columns.push(Arc::new(StringArray::from(decoded)) as ArrayRef);
                    let batch = RecordBatch::try_new(self.full_schema.clone(), columns)
                        .map_err(|error| decode(error.to_string()).with_source(error))?;
                    return match &self.projection {
                        Some(indices) => batch
                            .project(indices)
                            .map(Some)
                            .map_err(|error| decode(error.to_string()).with_source(error)),
                        None => Ok(Some(batch)),
                    };
                }
                self.current = None;
            }
            let Some(file) = self.files.pop_front() else {
                return Ok(None);
            };
            let decoded = self.source.clone().layer(DecompressionLayer::new(Gzip));
            self.current = Some(
                CsvAccessor::new(decoded, self.csv_schema.clone())
                    .with_format(CsvFormat::default().with_header(false))
                    .open(
                        ReadRequest::new(file)
                            .with_options(ReadOptions {
                                batch_size: self.options.batch_size,
                                projection: None,
                            })
                            .with_byte_options(self.byte_options),
                    )
                    .await?,
            );
        }
    }
}
#[async_trait]
impl<A: ByteAccessor + 'static> Reader for InventoryReader<A> {
    type Item = RecordBatch;
    type Metadata = RecordMetadata;
    fn capability(&self) -> Capability {
        capability(true)
    }
    fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }
    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        let result = self.read_batch().await;
        if !matches!(result, Ok(Some(_))) {
            self.done = true;
            self.current = None;
            self.files.clear();
        }
        result
    }
}
