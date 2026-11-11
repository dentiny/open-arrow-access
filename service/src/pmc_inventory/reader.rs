use std::sync::Arc;

use futures::{StreamExt, TryStreamExt, stream};
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteAccessor, ObjectRef, OrderRequirement, ReadRequest, RecordMetadata,
    RecordReader, SchemaRef, into_stream, record_stream_reader,
};
use open_arrow_access_decompression::{DecompressionLayer, gzip::Gzip};
use open_arrow_access_file_format::csv::CsvAccessor;

use super::validation;

#[derive(Clone)]
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
        let batches = readers.try_flatten().map(|batch| {
            let batch = batch?;
            validation::validate_batch(&batch)?;
            Ok(batch)
        });
        record_stream_reader(
            batches,
            metadata,
            self.output_projection,
            OrderRequirement::Reorderable,
        )
    }
}
