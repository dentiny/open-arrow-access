use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatchOptions, builder::StringBuilder, cast::AsArray};
use futures::{StreamExt, TryStreamExt, future, stream};
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteAccessor, ObjectRef, ReadOptions, ReadRequest, RecordBatch,
    RecordMetadata, RecordReader, Result, SchemaRef, StreamReader, into_stream,
};
use open_arrow_access_decompression::{DecompressionLayer, Gzip};
use open_arrow_access_file_format::{CsvAccessor, CsvFormat};

use super::{decode, validation};

pub(super) struct InventoryTransform {
    pub csv_schema: SchemaRef,
    pub projection: Vec<usize>,
    pub schema: SchemaRef,
}

impl InventoryTransform {
    pub fn create_reader<A: ByteAccessor + 'static>(
        self,
        source: Arc<A>,
        files: Vec<ObjectRef>,
        request: ReadRequest,
    ) -> impl RecordReader {
        let metadata = RecordMetadata {
            schema: self.schema.clone(),
            source: Some(request.object),
        };
        let csv = Arc::new(
            CsvAccessor::new(
                source.layer(DecompressionLayer::new(Gzip)),
                self.csv_schema.clone(),
            )
            .with_format(CsvFormat::default().with_header(false)),
        );
        let options = ReadOptions {
            batch_size: request.options.batch_size,
            projection: None,
        };
        let readers = stream::iter(files).then(move |file| {
            let csv = csv.clone();
            let request = ReadRequest::new(file)
                .with_options(options.clone())
                .with_byte_options(request.byte_options);
            async move { csv.open(request).await.map(into_stream) }
        });
        let batches = readers
            .try_flatten()
            .and_then(move |batch| future::ready(self.transform(batch)));
        StreamReader::new(batches, metadata)
    }

    fn transform(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let buckets = batch.column_by_name("Bucket").unwrap().as_string::<i32>();
        let keys = batch.column_by_name("Key").unwrap().as_string::<i32>();
        let metadata_index = self.csv_schema.fields().len();
        let mut decoded = self
            .projection
            .contains(&metadata_index)
            .then(|| StringBuilder::with_capacity(batch.num_rows(), keys.value_data().len()));
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
        // Avoid copying raw manifest metadata per batch.
        RecordBatch::try_new_with_options(
            self.schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )
        .map_err(|error| decode(error.to_string()).with_source(error))
    }
}
