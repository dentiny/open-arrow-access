use super::*;
use arrow_array::{BooleanArray, Int64Array, UInt64Array};
use open_arrow_access_core::{CompareOp, PredicateValue, PreparedSource, PruningPredicate};
use parquet::{
    arrow::ArrowWriter,
    file::properties::{EnabledStatistics, WriterProperties},
};

fn fixture(stats: EnabledStatistics) -> Vec<u8> {
    fixture_with_truncation(stats, None)
}

fn fixture_with_truncation(stats: EnabledStatistics, truncate: Option<usize>) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("unsigned", DataType::UInt64, false),
        Field::new("flag", DataType::Boolean, true),
        Field::new("text", DataType::Utf8, false),
    ]));
    let mut bytes = Vec::new();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(100))
        .set_dictionary_enabled(false)
        .set_statistics_truncate_length(truncate)
        .set_statistics_enabled(stats)
        .build();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema.clone(), Some(props)).unwrap();
    for group in 0..3 {
        let ids: Int64Array = (0..100)
            .map(|i| {
                if group == 2 {
                    None
                } else {
                    Some(group * 100 + i)
                }
            })
            .collect();
        let unsigned = UInt64Array::from_iter_values(
            (0..100).map(|i| u64::MAX - 299 + group as u64 * 100 + i),
        );
        let flags: BooleanArray = (0..100)
            .map(|_| if group == 2 { None } else { Some(group == 1) })
            .collect();
        let text = StringArray::from_iter_values(
            (0..100).map(|i| format!("{group}-{i:03}-{}", "x".repeat(2048))),
        );
        writer
            .write(
                &RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(ids),
                        Arc::new(unsigned),
                        Arc::new(flags),
                        Arc::new(text),
                    ],
                )
                .unwrap(),
            )
            .unwrap();
        writer.flush().unwrap();
    }
    writer.close().unwrap();
    bytes
}
fn compare(column: &str, op: CompareOp, value: PredicateValue) -> PruningPredicate {
    PruningPredicate::Compare {
        column: column.into(),
        op,
        value,
    }
}
async fn scan(
    data: &[u8],
    predicate: Option<PruningPredicate>,
    projection: Option<Vec<String>>,
    planned: bool,
) -> (usize, usize) {
    let observations = Arc::new(Observations::default());
    let accessor = ParquetAccessor::new(
        MemoryAccessor::new(data, 64)
            .with_ranges()
            .layer(ByteObserverLayer(observations.clone())),
    );
    let request = ReadRequest::new("data.parquet").with_options(ReadOptions {
        pruning_predicate: predicate,
        projection,
        ..ReadOptions::default()
    });
    let mut reader: Box<dyn RecordReader> = if planned {
        Box::new(accessor.plan_scan(request).await.unwrap().into_reader())
    } else {
        accessor.open(request).await.unwrap()
    };
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        rows += batch.num_rows();
    }
    (rows, observations.range_bytes.load(Ordering::SeqCst))
}
#[tokio::test]
async fn statistics_prune_data_io_and_preserve_residual_rows_in_open_and_plan() {
    let data = fixture(EnabledStatistics::Chunk);
    let (all, full_bytes) = scan(&data, None, None, false).await;
    assert_eq!(all, 300);
    for planned in [false, true] {
        let p = compare("id", CompareOp::Eq, PredicateValue::Int64(150));
        let (rows, bytes) = scan(&data, Some(p), None, planned).await;
        assert_eq!(
            rows, 100,
            "whole matching groups require residual filtering"
        );
        assert!(
            bytes < full_bytes / 2,
            "pruning must avoid data reads: {bytes} vs {full_bytes}"
        );
        let p = compare("id", CompareOp::Gt, PredicateValue::Int64(1000));
        assert_eq!(scan(&data, Some(p), None, planned).await.0, 0);
    }
}
#[tokio::test]
async fn conjunction_disjunction_nulls_unsigned_and_projection_are_conservative() {
    let data = fixture(EnabledStatistics::Chunk);
    let cases = [
        (
            compare("id", CompareOp::NotEq, PredicateValue::Int64(150)),
            200,
        ),
        (
            compare("id", CompareOp::Lt, PredicateValue::Int64(100)),
            100,
        ),
        (
            compare("id", CompareOp::LtEq, PredicateValue::Int64(100)),
            200,
        ),
        (
            compare("id", CompareOp::GtEq, PredicateValue::Int64(100)),
            100,
        ),
        (
            compare("unsigned", CompareOp::Eq, PredicateValue::UInt64(u64::MAX)),
            100,
        ),
        (
            compare("flag", CompareOp::Eq, PredicateValue::Boolean(true)),
            100,
        ),
        (PruningPredicate::IsNull("id".into()), 100),
        (PruningPredicate::IsNotNull("id".into()), 200),
        (
            PruningPredicate::And(vec![
                compare("id", CompareOp::Gt, PredicateValue::Int64(99)),
                compare("id", CompareOp::Lt, PredicateValue::Int64(200)),
            ]),
            100,
        ),
        (
            PruningPredicate::Or(vec![
                compare("id", CompareOp::Eq, PredicateValue::Int64(0)),
                PruningPredicate::IsNull("id".into()),
            ]),
            200,
        ),
        (
            compare("missing", CompareOp::Eq, PredicateValue::Int64(0)),
            300,
        ),
        (
            compare(
                "id",
                CompareOp::Eq,
                PredicateValue::Utf8("wrong type".into()),
            ),
            200,
        ),
    ];
    for (predicate, rows) in cases {
        for projection in [Some(vec!["text".into()]), Some(vec![])] {
            assert_eq!(
                scan(&data, Some(predicate.clone()), projection, false)
                    .await
                    .0,
                rows,
                "{predicate:?}"
            );
        }
    }
}
#[tokio::test]
async fn missing_statistics_and_unknown_or_branches_keep_all_groups() {
    let data = fixture(EnabledStatistics::None);
    assert_eq!(
        scan(
            &data,
            Some(compare("id", CompareOp::Eq, PredicateValue::Int64(1000))),
            None,
            false
        )
        .await
        .0,
        300
    );
    let data = fixture(EnabledStatistics::Chunk);
    let predicate = PruningPredicate::Or(vec![
        compare("id", CompareOp::Eq, PredicateValue::Int64(1000)),
        compare("missing", CompareOp::Eq, PredicateValue::Int64(1)),
    ]);
    assert_eq!(scan(&data, Some(predicate), None, false).await.0, 300);
}
#[tokio::test]
async fn prepared_scans_do_not_reuse_an_unpruned_reader_or_leak_hints_between_scans() {
    let data = fixture(EnabledStatistics::Chunk);
    let source = PreparedSource::prepare(
        ParquetAccessor::new(MemoryAccessor::new(&data, 64).with_ranges()),
        ReadRequest::new("data"),
    )
    .await
    .unwrap();
    for (predicate, expected) in [
        (
            Some(compare("id", CompareOp::Eq, PredicateValue::Int64(150))),
            100,
        ),
        (None, 300),
    ] {
        let mut reader = source
            .scan(ReadOptions {
                pruning_predicate: predicate,
                ..ReadOptions::default()
            })
            .await
            .unwrap();
        let mut rows = 0;
        while let Some(batch) = reader.next().await.unwrap() {
            rows += batch.num_rows();
        }
        assert_eq!(rows, expected);
    }
}

#[tokio::test]
async fn truncated_string_bounds_do_not_eliminate_groups() {
    let data = fixture_with_truncation(EnabledStatistics::Chunk, Some(4));
    let predicate = compare("text", CompareOp::Eq, PredicateValue::Utf8("zzz".into()));
    assert_eq!(scan(&data, Some(predicate), None, false).await.0, 300);
    let data = fixture(EnabledStatistics::Chunk);
    let predicate = compare("text", CompareOp::Eq, PredicateValue::Utf8("zzz".into()));
    assert_eq!(scan(&data, Some(predicate), None, false).await.0, 0);
}

#[tokio::test]
async fn schema_evolution_does_not_push_output_hints_into_changed_input_fields() {
    use open_arrow_access_layers::{ColumnEvolution, SchemaEvolutionLayer};
    let data = fixture(EnabledStatistics::Chunk);
    let accessor = ParquetAccessor::new(MemoryAccessor::new(&data, 64).with_ranges()).layer(
        SchemaEvolutionLayer::try_new(vec![
            ColumnEvolution::rename_column("id", "old_id"),
            ColumnEvolution::add_column_sql("id", "10000::BIGINT"),
        ])
        .unwrap(),
    );
    let predicate = compare("id", CompareOp::Eq, PredicateValue::Int64(10000));
    let mut reader = accessor
        .open(ReadRequest::new("data").with_options(ReadOptions {
            pruning_predicate: Some(predicate),
            ..ReadOptions::default()
        }))
        .await
        .unwrap();
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        rows += batch.num_rows();
    }
    assert_eq!(rows, 300);
}
