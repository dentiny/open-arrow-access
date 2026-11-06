use std::{
    num::NonZeroUsize,
    path::PathBuf,
    sync::{Arc, atomic::Ordering},
};

use arrow_array::{Array, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use more_asserts::assert_lt;
use open_arrow_access::{
    Accessor, ByteReadOptions, ErrorKind, ReadOptions, ReadRequest, RecordBatch, SchemaRef,
    file_format::JsonAccessor,
};

mod common;

use common::{MemoryAccessor, Observations};

fn schema() -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("title", DataType::Utf8, true),
        ],
        [("fixture".to_owned(), "json".to_owned())].into(),
    ))
}

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata/json")
        .join(name)
}

fn fixture_data(name: &str) -> Vec<u8> {
    let path = fixture_path(name);
    std::fs::read(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

fn fixture(name: &str, chunk_size: usize) -> (MemoryAccessor, Arc<Observations>) {
    let input = MemoryAccessor::new(&fixture_data(name), chunk_size);
    let observations = input.observations.clone();
    (input, observations)
}

fn request(batch_size: usize, projection: Option<Vec<&str>>) -> ReadRequest {
    ReadRequest::new("records.json").with_options(ReadOptions {
        batch_size: NonZeroUsize::new(batch_size).unwrap(),
        projection: projection.map(|names| names.into_iter().map(str::to_owned).collect()),
    })
}

fn ids(batch: &RecordBatch) -> &[i64] {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
}

#[tokio::test]
async fn fragmented_json_objects_preserve_values_schema_and_batch_order() {
    for chunk_size in [1, 7, 4096] {
        let (input, _) = fixture("good.json", chunk_size);
        let accessor = JsonAccessor::new(input, schema());
        let mut reader = accessor.open(request(2, None)).await.unwrap();
        assert_eq!(reader.metadata().schema, schema());
        assert_eq!(
            reader.metadata().source.as_ref().unwrap().locator(),
            "records.json"
        );
        let first = reader.next().await.unwrap().unwrap();
        assert_eq!(first.schema(), schema());
        assert_eq!(ids(&first), [0, 1]);
        let titles = first
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(titles.value(0), "雪\nline");
        assert!(titles.is_null(1));
        let last = reader.next().await.unwrap().unwrap();
        assert_eq!(ids(&last), [2, 3]);
        assert!(last.column(1).is_null(0));
        assert!(reader.next().await.unwrap().is_none());
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn projection_preserves_requested_order_metadata_and_zero_column_row_counts() {
    for projection in [vec!["title", "id"], vec![]] {
        let (input, _) = fixture("projection.jsonl", 3);
        let accessor = JsonAccessor::new(input, schema());
        let mut reader = accessor
            .open(request(1, Some(projection.clone())))
            .await
            .unwrap();
        for id in [1, 2] {
            let batch = reader.next().await.unwrap().unwrap();
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), projection.len());
            assert_eq!(batch.schema(), reader.metadata().schema);
            assert_eq!(batch.schema().metadata(), schema().metadata());
            if !projection.is_empty() {
                assert_eq!(batch.schema().field(0).name(), "title");
                assert_eq!(
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    id
                );
            }
        }
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn invalid_projection_fails_before_opening_bytes() {
    let (input, observed) = fixture("empty.json", 1);
    let accessor = JsonAccessor::new(input, schema());
    for projection in [vec!["missing"], vec!["id", "id"]] {
        let error = accessor
            .open(request(2, Some(projection)))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
    assert!(observed.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn malformed_truncated_and_non_object_inputs_fail_terminally() {
    for name in [
        "bad-empty-array.json",
        "bad-object-array.json",
        "bad-null.json",
        "bad-number.json",
        "bad-boolean.json",
        "bad-string.json",
        "bad-truncated-value.json",
        "bad-truncated-object.json",
        "bad-trailing.json",
        "bad-type.json",
        "bad-utf8.json",
    ] {
        for projection in [None, Some(vec![])] {
            // A projected-away field's value is deliberately not decoded.
            if matches!(name, "bad-type.json" | "bad-utf8.json") && projection.is_some() {
                continue;
            }
            let (input, observed) = fixture(name, 1);
            let accessor = JsonAccessor::new(input, schema());
            let mut reader = accessor.open(request(10, projection)).await.unwrap();
            let error = reader
                .next()
                .await
                .err()
                .unwrap_or_else(|| panic!("accepted {name}"));
            assert_eq!(error.kind(), ErrorKind::Decode, "{name}");
            let reads = observed.reads.load(Ordering::SeqCst);
            assert!(reader.next().await.unwrap().is_none());
            assert_eq!(observed.reads.load(Ordering::SeqCst), reads);
        }
    }
}

#[tokio::test]
async fn io_errors_keep_their_category_after_a_successful_batch() {
    let (mut input, observed) = fixture("io-failure.jsonl", 9);
    input.fail_after = Some(9);
    let accessor = JsonAccessor::new(input, schema());
    let mut reader = accessor.open(request(1, None)).await.unwrap();
    assert_eq!(ids(&reader.next().await.unwrap().unwrap()), [1]);
    let error = reader.next().await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::Io);
    assert_eq!(error.operation(), "memory_read");
    let reads = observed.reads.load(Ordering::SeqCst);
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(observed.reads.load(Ordering::SeqCst), reads);
}

#[tokio::test]
async fn per_open_options_are_forwarded_and_sequential_bytes_are_sufficient() {
    let (input, observed) = fixture("single.json", 1);
    let accessor = JsonAccessor::new(input, schema());
    assert!(accessor.capability().read);
    assert!(!accessor.capability().read_with_range);
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        io_chunk_size: NonZeroUsize::new(17),
    };
    let mut reader = accessor
        .open(request(1, None).with_byte_options(options))
        .await
        .unwrap();
    assert_eq!(observed.requests.lock().unwrap()[0].options, options);
    assert!(!reader.capability().read_with_range);
    assert_eq!(ids(&reader.next().await.unwrap().unwrap()), [1]);
}

#[tokio::test]
async fn reading_a_batch_does_not_collect_the_entire_input() {
    let data = fixture_data("many.jsonl");
    let input = MemoryAccessor::new(&data, 1);
    let observed = input.observations.clone();
    let accessor = JsonAccessor::new(input, schema());
    let mut reader = accessor.open(request(2, None)).await.unwrap();
    assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
    assert_eq!(ids(&reader.next().await.unwrap().unwrap()), [0, 1]);
    assert_lt!(observed.reads.load(Ordering::SeqCst), data.len());
    let reads = observed.reads.load(Ordering::SeqCst);
    drop(reader);
    assert_eq!(observed.reads.load(Ordering::SeqCst), reads);
}

#[tokio::test]
async fn empty_and_whitespace_only_inputs_have_no_rows() {
    for name in ["empty.json", "whitespace.json"] {
        let (input, observed) = fixture(name, 1);
        let accessor = JsonAccessor::new(input, schema());
        let mut reader = accessor.open(request(1, None)).await.unwrap();
        assert!(reader.next().await.unwrap().is_none());
        let reads = observed.reads.load(Ordering::SeqCst);
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(observed.reads.load(Ordering::SeqCst), reads);
    }
}

#[cfg(feature = "storage-fs")]
#[tokio::test]
async fn local_jsonl_reads_through_opendal() {
    let storage = open_arrow_access::storage::FsAccessor::new(fixture_path(""))
        .await
        .unwrap();
    let accessor = JsonAccessor::new(
        storage.with_chunk_size(NonZeroUsize::new(2).unwrap()),
        schema(),
    );
    let mut reader = accessor.open(ReadRequest::new("good.jsonl")).await.unwrap();
    assert_eq!(ids(&reader.next().await.unwrap().unwrap()), [1, 2]);
    assert!(reader.next().await.unwrap().is_none());
}
