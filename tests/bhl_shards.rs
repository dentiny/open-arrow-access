use std::path::Path;

use arrow_array::{RecordBatch, cast::AsArray};
use arrow_schema::DataType;
use open_arrow_access::{
    Accessor, ObjectRef,
    crawl::{CrawlAccessor, CrawlOptions},
    service::bhl::{BhlShardAccessor, BhlShardReadRequest, metadata::BhlMetadata, page_schema},
    storage::{NamedFileAccessor, OpenDalAccessor},
};
use open_arrow_access_test_support::{fixture_path, fs_operator, temp_fs};

async fn create_metadata(path: &Path) {
    let schema = page_schema();
    let DataType::Struct(tables) = schema.field_with_name("metadata").unwrap().data_type() else {
        unreachable!()
    };
    let files = tables.iter().map(|table| {
        (
            format!("data/{}.txt.gz", table.name()),
            ObjectRef::new(format!("{}.txt.gz", table.name())),
        )
    });
    let storage = OpenDalAccessor::new(fs_operator(fixture_path!("bhl-metadata", "")));
    BhlMetadata::create(
        NamedFileAccessor::new(storage, files.collect()).unwrap(),
        path,
    )
    .await
    .unwrap();
}

async fn read(crawl: &CrawlAccessor<BhlShardReadRequest>, prefix: &str) -> RecordBatch {
    let mut reader = crawl
        .open(BhlShardReadRequest::new(prefix).unwrap())
        .await
        .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert!(reader.next().await.unwrap().is_none());
    batch
}

#[tokio::test]
async fn crawl_reuses_a_completed_database_for_item_and_part_shards() {
    let item = "ocr/item-2/item-2-3-5.txt";
    let part = "ocr/part-8/part-8-3-1.txt";
    let (root, operator) = temp_fs(&[
        (item, b"\r\n\xff"),
        (part, b""),
        ("ocr/item-2/item-2.txt", b"aggregate"),
    ]);
    let database = root.path().join("metadata.duckdb");
    create_metadata(&database).await;
    // This bucket mirror has OCR only, so an implicit TSV import would fail.
    let source = BhlShardAccessor::from_database(OpenDalAccessor::new(operator), &database)
        .await
        .unwrap();
    let crawl = CrawlAccessor::new(source, CrawlOptions::default());
    let (item_batch, part_batch) =
        tokio::join!(read(&crawl, "ocr/item-2/"), read(&crawl, "ocr/part-8/"));
    for (batch, key, data) in [
        (item_batch, item, b"\r\n\xff".as_slice()),
        (part_batch, part, b"".as_slice()),
    ] {
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch["source_path"].as_string::<i64>().value(0),
            format!("s3://bhl-open-data/{key}")
        );
        assert_eq!(batch["data"].as_binary::<i64>().value(0), data);
        assert_eq!(
            batch["snapshot_id"].as_string::<i32>().value(0),
            batch.schema().metadata()["bhl.metadata.fingerprint"]
        );
    }
}

#[tokio::test]
async fn opening_a_missing_metadata_database_never_creates_it() {
    let (root, operator) = temp_fs(&[]);
    let path = root.path().join("missing.duckdb");
    assert!(
        BhlShardAccessor::from_database(OpenDalAccessor::new(operator), &path)
            .await
            .is_err()
    );
    assert!(!path.exists());
}
