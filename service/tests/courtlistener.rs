use std::{num::NonZeroUsize, sync::Arc};

use arrow_array::cast::AsArray;
use arrow_schema::{DataType, Field, Schema};
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteReadOptions, ByteReadRequest, ErrorKind, Result, ScanPlanner,
};
use open_arrow_access_dataset::file_set::FileSetRequest;
use open_arrow_access_file_format::csv::{CsvFileFormat, CsvFormat};
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_service::courtlistener::{CourtListenerBulkAccessor, CourtListenerSnapshot};
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{MemoryAccessor, collect_bytes, fixture, fs_operator};

#[tokio::test]
async fn discovers_only_exact_table_snapshots_in_date_order() -> Result<()> {
    let root = tempfile::tempdir().unwrap();
    let bulk = root.path().join("bulk-data");
    tokio::fs::create_dir_all(bulk.join("nested"))
        .await
        .unwrap();
    // Empty files make any accidental attempt to decode table contents fail.
    for name in [
        "opinions-2026-09-30.csv.bz2",
        "opinions-2026-03-31.csv.bz2",
        "opinions-cited-2026-09-30.csv.bz2",
        "nested/opinions-2026-09-30.csv.bz2",
        "opinions-2026-02-30.csv.bz2",
        "opinions-2026-09-30.csv.bz2.tmp",
        "schema-2026-09-30.sql",
    ] {
        tokio::fs::write(bulk.join(name), b"").await.unwrap();
    }
    let source = CourtListenerBulkAccessor::new(OpenDalAccessor::new(fs_operator(root.path())));
    let snapshots = source.snapshots("opinions").await?;
    assert_eq!(
        snapshots,
        [
            CourtListenerSnapshot::new("opinions", "2026-03-31")?,
            CourtListenerSnapshot::new("opinions", "2026-09-30")?,
        ]
    );
    assert_eq!(snapshots[1].table(), "opinions");
    assert_eq!(snapshots[1].date(), "2026-09-30");
    assert_eq!(
        snapshots[1].schema_object().locator(),
        "bulk-data/schema-2026-09-30.sql"
    );
    assert!(source.snapshots("courts").await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn explicit_read_reuses_bzip2_and_forwards_io_options() -> Result<()> {
    let input = MemoryAccessor::new(
        &fixture!("courtlistener", "courts-2026-09-30.csv.bz2").await,
        1,
    );
    let observed = input.observations.clone();
    let source = CourtListenerBulkAccessor::new(input);
    assert_eq!(
        source.snapshots("courts").await.unwrap_err().kind(),
        ErrorKind::Unsupported
    );
    let snapshot = CourtListenerSnapshot::new("courts", "2026-09-30")?;
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        io_chunk_size: NonZeroUsize::new(7),
    };
    let mut reader = source
        .open(ByteReadRequest::new(snapshot.object()).with_options(options))
        .await?;
    assert_eq!(reader.metadata().source, Some(snapshot.object()));
    assert_eq!(reader.metadata().content_length, None);
    assert!(!source.capability().list);
    assert!(!source.capability().read_with_range);
    assert!(!reader.capability().read_with_range);
    assert_eq!(
        collect_bytes(reader.as_mut()).await?,
        fixture!("courtlistener", "courts-2026-09-30.csv").await
    );
    let requests = observed.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].object, snapshot.object());
    assert_eq!(requests[0].options, options);
    Ok(())
}

#[tokio::test]
async fn invalid_selections_fail_before_storage_io() {
    let input = MemoryAccessor::new(b"", 1);
    let observed = input.observations.clone();
    let source = CourtListenerBulkAccessor::new(input);
    for (table, date) in [
        ("", "2026-09-30"),
        ("../courts", "2026-09-30"),
        ("Courts", "2026-09-30"),
        ("courts", "0000-01-01"),
        ("courts", "1900-02-29"),
        ("courts", "2026-02-29"),
        ("courts", "2026-04-31"),
        ("courts", "2026-00-01"),
        ("courts", "2026-13-01"),
        ("courts", "2026-01-00"),
        ("courts", "2026-1-01"),
    ] {
        assert!(CourtListenerSnapshot::new(table, date).is_err());
    }
    assert!(CourtListenerSnapshot::new("people_db_race", "2000-02-29").is_ok());
    assert_eq!(
        source.snapshots("../courts").await.unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    for object in [
        "bulk-data/../courts-2026-09-30.csv.bz2",
        "bulk-data/courts-2026-02-30.csv.bz2",
        "courts-2026-09-30.csv.bz2",
        "bulk-data/schema-2026-09-30.sql",
        "bulk-data/💥-2026-09-30.csv.bz2",
    ] {
        assert_eq!(
            source.open(object.into()).await.err().unwrap().kind(),
            ErrorKind::InvalidInput
        );
    }
    assert!(observed.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn snapshot_record_plans_preserve_validation_order_and_quoted_csv() {
    let raw = fixture!("courtlistener", "courts-2026-09-30.csv").await;
    let fields = std::str::from_utf8(&raw)
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .split(',')
        .map(|name| Field::new(name, DataType::Utf8, true))
        .collect::<Vec<_>>();
    let bytes = MemoryAccessor::new(
        &fixture!("courtlistener", "courts-2026-09-30.csv.bz2").await,
        1,
    );
    let observed = bytes.observations.clone();
    let records = CourtListenerBulkAccessor::new(bytes).records(
        CsvFileFormat::new(Arc::new(Schema::new(fields)))
            .with_format(CsvFormat::default().with_header(true)),
    );
    let bad = FileSetRequest::new(["bulk-data/../courts-2026-09-30.csv.bz2"]);
    assert_eq!(
        records.plan_scan(bad).await.err().unwrap().kind(),
        ErrorKind::InvalidInput
    );
    assert!(observed.requests.lock().unwrap().is_empty());
    let key = CourtListenerSnapshot::new("courts", "2026-09-30")
        .unwrap()
        .object();
    let selection = FileSetRequest::new([key.clone(), key]);
    let plan = records.plan_scan(selection.clone()).await.unwrap();
    assert_eq!(plan.partitions.size_hint().0, 2);
    drop(plan);
    let mut reader = records
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ))
        .open(selection)
        .await
        .unwrap();
    let mut ids = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        ids.extend(
            batch["id"]
                .as_string::<i32>()
                .iter()
                .map(|value| value.unwrap().to_owned()),
        );
    }
    assert_eq!(
        ids,
        ["nc", "minnag", "arizsuper", "nc", "minnag", "arizsuper"]
    );
}
