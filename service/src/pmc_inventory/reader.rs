use std::sync::Arc;

use arrow_array::{Array, cast::AsArray};
use futures::{StreamExt, TryStreamExt, stream};
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteAccessor, ObjectRef, ReadRequest, RecordBatch, RecordMetadata,
    RecordReader, Result, SchemaRef, StreamReader, into_stream, project_batch,
};
use open_arrow_access_decompression::{DecompressionLayer, gzip::Gzip};
use open_arrow_access_file_format::csv::CsvAccessor;

use super::validation;

pub(super) struct InventoryTransform {
    pub(super) csv_schema: SchemaRef,
    pub(super) output_projection: Option<Vec<usize>>,
    pub(super) output_schema: SchemaRef,
}

impl InventoryTransform {
    pub(super) fn create_reader<A: ByteAccessor + 'static>(
        self,
        source: Arc<A>,
        files: Vec<ObjectRef>,
        request: ReadRequest,
    ) -> impl RecordReader {
        let metadata = RecordMetadata {
            schema: self.output_schema.clone(),
            source: Some(request.object),
        };
        // S3 Inventory CSV shards are gzip-compressed (*.csv.gz).
        let csv = Arc::new(CsvAccessor::new(
            source.layer(DecompressionLayer::new(Gzip)),
            self.csv_schema.clone(),
        ));
        let options = request.options;
        let readers = stream::iter(files).then(move |file| {
            let csv = csv.clone();
            let request = ReadRequest::new(file)
                .with_options(options.clone())
                .with_byte_options(request.byte_options);
            async move { csv.open(request).await.map(into_stream) }
        });
        let batches = readers
            .try_flatten()
            .map(move |batch| batch.and_then(|batch| self.transform(batch)));
        StreamReader::new(batches, metadata)
    }

    fn transform(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let buckets = batch.column_by_name("Bucket").unwrap().as_string::<i32>();
        let keys = batch.column_by_name("Key").unwrap().as_string::<i32>();
        for row in 0..batch.num_rows() {
            validation::validate_row(
                (!buckets.is_null(row)).then(|| buckets.value(row)),
                (!keys.is_null(row)).then(|| keys.value(row)),
            )?;
        }
        project_batch(
            batch,
            self.output_projection.as_deref(),
            self.output_schema.clone(),
        )
    }
}
