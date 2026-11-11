use std::{num::NonZeroUsize, sync::Arc};

use arrow_array::Int64Array;
use arrow_schema::{DataType, Field, Schema};
use open_arrow_access_core::{Accessor, AccessorExt, ReadOptions, ReadRequest};
use open_arrow_access_file_format::csv::CsvAccessor;
use open_arrow_access_layers::SamplingLayer;
use open_arrow_access_test_support::MemoryAccessor;

#[tokio::test]
async fn sampling_rate_one_keeps_every_row() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let source = CsvAccessor::new(MemoryAccessor::new(b"0\n1\n2\n3\n4\n", 2), schema)
        .layer(SamplingLayer::try_new(1.0, 42).unwrap());
    let request = ReadRequest::new("input.csv").with_options(ReadOptions {
        batch_size: NonZeroUsize::new(2).unwrap(),
        ..Default::default()
    });
    let mut reader = source.open(request).await.unwrap();
    let mut ids = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        ids.extend_from_slice(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
        );
    }
    assert_eq!(ids, [0, 1, 2, 3, 4]);
}
