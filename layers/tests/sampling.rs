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

#[tokio::test]
async fn seeded_sampling_is_lazy_and_independent_of_batches_and_projection() {
    use open_arrow_access_file_format::csv::CsvFormat;
    use open_arrow_access_test_support::{fixture, reader::assert_finished};
    use std::sync::atomic::Ordering;

    let bytes = fixture!("csv", "good.csv").await;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("title", DataType::Utf8, true),
    ]));
    for batch_size in [1, 2, 8] {
        for projection in [None, Some(vec![])] {
            let input = MemoryAccessor::new(&bytes, 1);
            let observed = input.observations.clone();
            let source = CsvAccessor::new(input, schema.clone())
                .with_format(CsvFormat::default().with_header(true))
                .layer(SamplingLayer::try_new(0.5, 42).unwrap());
            // Each open restarts the same seeded stream.
            for _ in 0..2 {
                let polls = observed.reads.load(Ordering::SeqCst);
                let mut reader = source
                    .open(ReadRequest::new("input.csv").with_options(ReadOptions {
                        batch_size: NonZeroUsize::new(batch_size).unwrap(),
                        projection: projection.clone(),
                    }))
                    .await
                    .unwrap();
                assert_eq!(observed.reads.load(Ordering::SeqCst), polls);
                assert_eq!(reader.capability(), source.capability());
                assert_eq!(
                    reader.metadata().source.as_ref().unwrap().locator(),
                    "input.csv"
                );
                let mut rows = 0;
                let mut ids = Vec::new();
                while let Some(batch) = reader.next().await.unwrap() {
                    assert!(batch.num_rows() > 0);
                    rows += batch.num_rows();
                    if projection.is_none() {
                        ids.extend_from_slice(
                            batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .values(),
                        );
                    } else {
                        assert_eq!(batch.num_columns(), 0);
                    }
                }
                assert_eq!(rows, 4);
                if projection.is_none() {
                    assert_eq!(ids, [2, 3, 4, 5]);
                }
                assert_finished(reader.as_mut(), &observed.reads, |m| {
                    (m.schema.clone(), m.source.clone())
                })
                .await;
            }
        }
    }
}

#[tokio::test]
async fn zero_sampling_never_polls_and_source_errors_are_terminal() {
    use open_arrow_access_core::ErrorKind;
    use open_arrow_access_test_support::{
        fixture,
        reader::{assert_finished, assert_terminal_error},
    };
    use std::sync::atomic::Ordering;

    let bytes = fixture!("csv", "good_no_header.csv").await;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("title", DataType::Utf8, true),
    ]));
    for probability in [0.0, 0.5] {
        let input = MemoryAccessor::new(&bytes, 1).with_failure(0, ErrorKind::PermissionDenied);
        let observed = input.observations.clone();
        let source = CsvAccessor::new(input, schema.clone())
            .layer(SamplingLayer::try_new(probability, 42).unwrap());
        let mut reader = source.open(ReadRequest::new("input.csv")).await.unwrap();
        assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
        if probability == 0.0 {
            assert_finished(reader.as_mut(), &observed.reads, |m| {
                (m.schema.clone(), m.source.clone())
            })
            .await;
            assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
        } else {
            let error = assert_terminal_error(
                reader.as_mut(),
                &observed.reads,
                ErrorKind::PermissionDenied,
                |m| (m.schema.clone(), m.source.clone()),
            )
            .await;
            assert_eq!(error.operation(), "memory_read");
        }
    }
}
