use super::{decode, validation};
use arrow_array::{Array, ArrayRef, RecordBatchOptions, StringArray, builder::StringBuilder};
use futures::stream;
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteAccessor, ByteReadOptions, ObjectRef, ReadOptions, ReadRequest,
    RecordBatch, RecordMetadata, RecordReader, Result, SchemaRef, StreamReader,
};
use open_arrow_access_decompression::{DecompressionLayer, Gzip};
use open_arrow_access_file_format::{CsvAccessor, CsvFormat};
use std::{collections::VecDeque, sync::Arc};

pub(super) struct InventoryState<A> {
    pub source: Arc<A>,
    pub files: VecDeque<ObjectRef>,
    pub current: Option<Box<dyn RecordReader>>,
    pub csv_schema: SchemaRef,
    pub projection: Vec<usize>,
    pub options: ReadOptions,
    pub byte_options: ByteReadOptions,
    pub schema: SchemaRef,
}
impl<A: ByteAccessor + 'static> InventoryState<A> {
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
                    let metadata_index = self.csv_schema.fields().len();
                    let mut decoded = self.projection.contains(&metadata_index).then(|| {
                        StringBuilder::with_capacity(batch.num_rows(), keys.value_data().len())
                    });
                    for row in 0..batch.num_rows() {
                        let key = validation::validate_row(
                            (!buckets.is_null(row)).then(|| buckets.value(row)),
                            (!keys.is_null(row)).then(|| keys.value(row)),
                        )?;
                        if let Some(builder) = &mut decoded {
                            builder.append_value(key);
                        }
                    }
                    let decoded = decoded.map(|mut builder| Arc::new(builder.finish()) as ArrayRef);
                    let columns = self
                        .projection
                        .iter()
                        .map(|&index| {
                            if index == metadata_index {
                                decoded.as_ref().unwrap().clone()
                            } else {
                                batch.column(index).clone()
                            }
                        })
                        .collect();
                    // Reuse the output schema, including the potentially large raw manifest.
                    return RecordBatch::try_new_with_options(
                        self.schema.clone(),
                        columns,
                        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
                    )
                    .map(Some)
                    .map_err(|error| decode(error.to_string()).with_source(error));
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
/// Keep shard iteration local and share reader lifecycle handling.
pub(super) fn create_reader<A: ByteAccessor + 'static>(
    state: InventoryState<A>,
    metadata: RecordMetadata,
) -> impl RecordReader {
    let stream = stream::try_unfold(state, |mut state| async move {
        Ok(state.read_batch().await?.map(|batch| (batch, state)))
    });
    StreamReader::new(stream, metadata)
}
