use std::sync::Arc;

use arrow_array::{Array, cast::AsArray};
use futures::{StreamExt, TryStreamExt, stream};
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteAccessor, ObjectRef, ReadRequest, RecordBatch, RecordMetadata,
    RecordReader, Result, SchemaRef, StreamReader, into_stream, project_batch,
};
use open_arrow_access_decompression::{DecompressionLayer, Gzip};
use open_arrow_access_file_format::{CsvAccessor, CsvFormat};

use super::validation;

pub(super) struct InventoryTransform {
    pub csv_schema: SchemaRef,
    pub projection: Option<Vec<usize>>,
    pub schema: SchemaRef,
}

impl InventoryTransform {
    pub fn create_reader<A: ByteAccessor + 'static>(
        mut self,
        source: Arc<A>,
        files: Vec<ObjectRef>,
        request: ReadRequest,
    ) -> impl RecordReader {
        let metadata = RecordMetadata {
            schema: self.schema.clone(),
            source: Some(request.object),
        };
        // S3 Inventory CSV shards are gzip-compressed (*.csv.gz).
        let csv = Arc::new(
            CsvAccessor::new(
                source.layer(DecompressionLayer::new(Gzip)),
                self.csv_schema.clone(),
            )
            .with_format(CsvFormat::default().with_header(false)),
        );
        let mut options = request.options;
        if let Some(columns) = options.projection.as_mut() {
            // Output columns come first; validation-only columns are appended.
            self.projection = Some((0..columns.len()).collect());
            for name in ["Bucket", "Key"] {
                if !columns.iter().any(|column| column == name) {
                    columns.push(name.into());
                }
            }
        }
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
        project_batch(batch, self.projection.as_deref(), self.schema.clone())
    }
}
