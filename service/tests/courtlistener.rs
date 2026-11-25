use std::{num::NonZeroUsize, sync::Arc};

use arrow_array::{
    Array, BooleanArray, Float64Array, Int16Array, Int64Array, StringArray, StructArray,
    TimestampMicrosecondArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::concat::concat_batches;
use futures::AsyncWriteExt;
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteReadOptions, ByteReadRequest, ErrorKind, ReadRequest, RecordReader,
    Result, ScanPlanner,
};
use open_arrow_access_dataset::file_set::FileSetRequest;
use open_arrow_access_file_format::csv::{CsvFileFormat, CsvFormat};
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_service::courtlistener::{
    CourtListenerBulkAccessor, CourtListenerDatePartitionAccessor, CourtListenerSnapshot,
    CourtListenerTableAccessor,
};
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
    let reader = records
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ))
        .open(selection)
        .await
        .unwrap();
    let ids = open_arrow_access_test_support::record::collect_strings::<i32>(reader, 0).await;
    assert_eq!(
        ids,
        ["nc", "minnag", "arizsuper", "nc", "minnag", "arizsuper"]
    );
}

async fn write_bz(root: &std::path::Path, table: &str, data: &str) {
    let mut encoder = async_compression::futures::write::BzEncoder::new(Vec::new());
    encoder.write_all(data.as_bytes()).await.unwrap();
    encoder.close().await.unwrap();
    tokio::fs::write(
        root.join(format!("bulk-data/{table}-2026-09-30.csv.bz2")),
        encoder.into_inner(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn typed_tables_preserve_sql_types_and_copy_nulls() -> Result<()> {
    let input = fixture!("courtlistener", "courts-2026-09-30.csv.bz2").await;
    let source = CourtListenerTableAccessor::new(MemoryAccessor::new(&input, 1));
    let mut reader = source
        .open(ReadRequest::new("bulk-data/courts-2026-09-30.csv.bz2"))
        .await?;
    let batch = reader.next().await?.unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 20);
    let column = |name: &str| batch.column_by_name(name).unwrap();
    assert_eq!(
        column("id")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "nc"
    );
    assert_eq!(
        column("position")
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        366.97
    );
    assert_eq!(
        column("pacer_court_id")
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .null_count(),
        3
    );
    assert_eq!(
        column("fjc_court_id")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        ""
    );
    assert!(
        column("notes")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .contains("\r\n")
    );
    assert_eq!(
        column("date_modified")
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .null_count(),
        0
    );
    assert!(reader.next().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn typed_table_plans_are_lazy_and_reject_drift_and_invalid_values() -> Result<()> {
    let root = tempfile::tempdir().unwrap();
    tokio::fs::create_dir(root.path().join("bulk-data"))
        .await
        .unwrap();
    let source = CourtListenerTableAccessor::new(OpenDalAccessor::new(fs_operator(root.path())));
    let req = ReadRequest::new("bulk-data/courts-2026-09-30.csv.bz2");
    assert_eq!(
        source
            .plan_scan(req.clone())
            .await?
            .partitions
            .size_hint()
            .0,
        1
    );
    assert!(
        source
            .open(req.clone().with_projection(["missing"]))
            .await
            .is_err()
    );
    let fixture = fixture!("courtlistener", "courts-2026-09-30.csv").await;
    let raw = std::str::from_utf8(&fixture).unwrap();
    for invalid in [raw.replacen("id,", "unknown,", 1), String::new()] {
        write_bz(root.path(), "courts", &invalid).await;
        assert!(source.open(req.clone()).await.is_err());
    }
    write_bz(root.path(), "courts", &raw.replace("\"366.97\"", "\"NaN\"")).await;
    let mut reader = source.open(req.with_projection(["id"])).await?;
    assert!(reader.next().await.is_err());
    assert!(reader.next().await?.is_none());
    Ok(())
}

const JOIN_FIXTURES: [(&str, &str); 4] = [
    (
        "opinions",
        include_str!("../../testdata/courtlistener/joined/opinion.csv"),
    ),
    (
        "opinion-clusters",
        include_str!("../../testdata/courtlistener/joined/cluster.csv"),
    ),
    (
        "dockets",
        include_str!("../../testdata/courtlistener/joined/docket.csv"),
    ),
    (
        "courts",
        include_str!("../../testdata/courtlistener/joined/court.csv"),
    ),
];

async fn joined_fixture() -> (
    tempfile::TempDir,
    CourtListenerDatePartitionAccessor<OpenDalAccessor>,
) {
    let root = tempfile::tempdir().unwrap();
    tokio::fs::create_dir(root.path().join("bulk-data"))
        .await
        .unwrap();
    for (table, data) in JOIN_FIXTURES {
        write_bz(root.path(), table, data).await;
    }
    let source = CourtListenerDatePartitionAccessor::new(
        OpenDalAccessor::new(fs_operator(root.path())),
        "s3://com-courtlistener-storage",
    )
    .unwrap();
    (root, source)
}
async fn consume(mut reader: Box<dyn RecordReader>) -> Result<usize> {
    let mut count = 0;
    while let Some(batch) = reader.next().await? {
        count += batch.num_rows();
    }
    Ok(count)
}

#[tokio::test]
async fn documents_preserve_all_representations_and_defer_downloads_to_sink() -> Result<()> {
    let (root, source) = joined_fixture().await;
    write_bz(
        root.path(),
        "opinions",
        include_str!("../../testdata/courtlistener/documents/opinion.csv"),
    )
    .await;
    write_bz(
        root.path(),
        "opinion-clusters",
        include_str!("../../testdata/courtlistener/documents/cluster.csv"),
    )
    .await;
    // There are no media files, and .invalid URLs must never be fetched by this reader.
    let expected_schema = source.schemas().schema_with_document_urls();
    let source = source.with_document_urls("https://media.invalid/base")?;
    let request = ReadRequest::new("2026-09-30").with_batch_size(NonZeroUsize::new(2).unwrap());
    let schema = source.schema();
    assert_eq!(schema, expected_schema);
    assert_eq!(schema.fields().len(), 7);
    assert_eq!(
        source
            .plan_scan(request.clone())
            .await?
            .partitions
            .size_hint()
            .0,
        1
    );
    let mut reader = source.open(request).await?;
    let mut batches = Vec::new();
    while let Some(batch) = reader.next().await? {
        assert!(batch.num_rows() <= 2);
        batches.push(batch);
    }
    let batch = concat_batches(&schema, &batches).unwrap();
    assert_eq!(batch.num_rows(), 5);
    let opinion = batch
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(opinion.num_columns(), 22);
    assert_eq!(
        child::<StringArray>(opinion, "plain_text").value(0),
        "line1\nline2 \"quoted\" \\ slash"
    );
    assert_eq!(
        child::<StringArray>(opinion, "xml_harvard").value(0),
        "<opinion>XML &amp; text</opinion>"
    );
    assert_eq!(
        child::<StringArray>(opinion, "xml_scan").value(0),
        "<scan>different representation</scan>"
    );
    assert_eq!(
        child::<StringArray>(opinion, "html").value(0),
        "<p>HTML</p>"
    );
    assert_eq!(
        child::<StringArray>(opinion, "local_path").value(0),
        "pdf/opinion 1.pdf"
    );
    let documents = batch
        .column(6)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let files = child::<StringArray>(documents, "opinion_file_url");
    let pdfs = child::<StringArray>(documents, "cluster_pdf_url");
    assert_eq!(
        files.value(0),
        "https://media.invalid/base/pdf/opinion%201.pdf"
    );
    assert_eq!(
        files.value(1),
        "https://publisher.invalid/opinion.pdf?sig=a%2Fb"
    );
    assert_eq!(
        files.value(2),
        "https://mirror.invalid/original.pdf?sig=x%2Fy"
    );
    assert!(files.is_null(3));
    assert_eq!(
        pdfs.value(0),
        "https://media.invalid/base/harvard_pdf/cluster.pdf"
    );
    assert_eq!(pdfs.value(4), pdfs.value(0)); // Same cluster PDF shared by two opinions.
    assert!(pdfs.is_null(1)); // Missing cluster.
    assert!(pdfs.is_null(2)); // Present cluster, absent PDF path.
    assert!(reader.next().await?.is_none());
    let mut projected = source
        .open(ReadRequest::new("2026-09-30").with_projection(["documents", "opinion"]))
        .await?;
    assert_eq!(
        projected.next().await?.unwrap().schema().field(0).name(),
        "documents"
    );
    assert_eq!(
        consume(
            source
                .open(ReadRequest::new("2026-09-30").with_projection(Vec::<String>::new()))
                .await?
        )
        .await?,
        5
    );
    Ok(())
}

#[tokio::test]
async fn document_plans_are_lazy_and_invalid_references_fail_without_dropping_rows() -> Result<()> {
    for root in [
        "",
        "file:///tmp/media",
        "https://user:password@example/media",
        "https://example/media?x=1",
        "https://example/media#x",
    ] {
        let bytes = MemoryAccessor::new(b"", 1);
        assert!(
            CourtListenerDatePartitionAccessor::new(bytes, "s3://source")?
                .with_document_urls(root)
                .is_err()
        );
    }
    let bytes = MemoryAccessor::new(b"", 1);
    let observed = bytes.observations.clone();
    let source = CourtListenerDatePartitionAccessor::new(bytes, "s3://source")?
        .with_document_urls("https://media.invalid/")?;
    source.plan_scan(ReadRequest::new("2026-09-30")).await?;
    assert!(
        source
            .metadata(ReadRequest::new("2026-09-30").with_projection(["missing"]))
            .await
            .is_err()
    );
    assert!(
        source
            .plan_scan(ReadRequest::new("2026-02-30"))
            .await
            .is_err()
    );
    assert!(observed.requests.lock().unwrap().is_empty());
    let (root, source) = joined_fixture().await;
    let raw = include_str!("../../testdata/courtlistener/documents/opinion.csv")
        .replace("pdf/opinion 1.pdf", "../escape.pdf");
    write_bz(root.path(), "opinions", &raw).await;
    let source = source.with_document_urls("https://media.invalid/")?;
    let mut reader = source.open(ReadRequest::new("2026-09-30")).await?;
    assert!(reader.next().await.is_err());
    assert!(reader.next().await?.is_none());
    Ok(())
}
#[tokio::test]
async fn historical_local_paths_preserve_rows_and_fall_back_to_download_url() -> Result<()> {
    let (root, source) = joined_fixture().await;
    let raw = include_str!("../../testdata/courtlistener/documents/opinion.csv")
        .replace("pdf/opinion 1.pdf", "/home/importer/archive/original.xml")
        .replace(
            "https://mirror.invalid/original.pdf?sig=x%2Fy",
            "/home/importer/archive/other.xml",
        );
    write_bz(root.path(), "opinions", &raw).await;
    let source = source.with_document_urls("https://media.invalid/")?;
    let mut reader = source.open(ReadRequest::new("2026-09-30")).await?;
    let schema = reader.metadata().schema.clone();
    let mut batches = Vec::new();
    while let Some(batch) = reader.next().await? {
        batches.push(batch);
    }
    let batch = concat_batches(&schema, &batches).unwrap();
    assert_eq!(batch.num_rows(), 5);
    let docs = batch
        .column_by_name("documents")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let urls = child::<StringArray>(docs, "opinion_file_url");
    assert!(urls.is_null(0));
    assert_eq!(urls.value(2), "https://publisher.invalid/unused.pdf");
    let opinion = batch
        .column_by_name("opinion")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(
        child::<StringArray>(opinion, "local_path").value(0),
        "/home/importer/archive/original.xml"
    );
    assert_eq!(
        child::<StringArray>(opinion, "xml_harvard").value(0),
        "<opinion>XML &amp; text</opinion>"
    );
    Ok(())
}
fn child<'a, T: Array + 'static>(s: &'a StructArray, name: &str) -> &'a T {
    s.column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<T>()
        .unwrap()
}
#[tokio::test]
async fn joined_date_has_typed_nested_rows_null_joins_and_exact_provenance() -> Result<()> {
    let (_root, source) = joined_fixture().await;
    let mut reader = source.open(ReadRequest::new("2026-09-30")).await?;
    let schema = reader.metadata().schema.clone();
    assert_eq!(schema.fields().len(), 6);
    let mut batches = Vec::new();
    while let Some(batch) = reader.next().await? {
        batches.push(batch);
    }
    let batch = concat_batches(&schema, &batches).unwrap();
    assert_eq!(batch.num_rows(), 5);
    let opinion = batch
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(opinion.num_columns(), 22);
    assert_eq!(
        child::<Int64Array>(opinion, "id").values(),
        &[1, 2, 3, 4, 5]
    );
    assert_eq!(child::<UInt64Array>(opinion, "page_count").value(0), 12);
    assert!(child::<UInt64Array>(opinion, "page_count").is_null(1));
    assert!(child::<BooleanArray>(opinion, "per_curiam").value(0));
    assert_eq!(
        child::<StringArray>(opinion, "plain_text").value(0),
        "line1\nline2 \"quoted\" \\ slash"
    );
    let times = child::<TimestampMicrosecondArray>(opinion, "date_created");
    assert_eq!(times.value(0) - times.value(1), 123456);
    let urls = child::<StringArray>(opinion, "download_url");
    assert!(urls.is_null(0));
    assert!(!urls.is_null(1));
    assert_eq!(urls.value(1), "");
    let cluster = batch
        .column(2)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let docket = batch
        .column(3)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let court = batch
        .column(4)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    // The full 2026-09-30 export contains citation_count = -1.
    assert_eq!(child::<Int64Array>(cluster, "citation_count").value(0), -1);
    assert!(cluster.is_null(1));
    assert!(!cluster.is_null(2));
    assert!(docket.is_null(1));
    assert!(docket.is_null(2));
    assert!(!docket.is_null(3));
    assert!(court.is_null(3));
    assert_eq!(child::<StringArray>(court, "id").value(0), "001");
    assert_eq!(child::<StringArray>(court, "fjc_court_id").value(0), "003");
    assert_eq!(child::<Float64Array>(court, "position").value(0), 1.5);
    assert_eq!(child::<Int64Array>(cluster, "id").value(4), 10);
    let provenance = batch
        .column(5)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(
        child::<UInt64Array>(provenance, "opinion_record_index").values(),
        &[0, 1, 2, 3, 4]
    );
    assert!(child::<UInt64Array>(provenance, "cluster_record_index").is_null(1));
    assert_eq!(
        child::<UInt64Array>(provenance, "cluster_record_index").value(4),
        0
    );
    assert_eq!(
        child::<StringArray>(provenance, "courts_source_uri").value(0),
        "s3://com-courtlistener-storage/bulk-data/courts-2026-09-30.csv.bz2"
    );
    assert!(reader.next().await?.is_none());
    assert!(reader.next().await?.is_none());
    Ok(())
}
#[tokio::test]
async fn date_planning_is_lazy_and_projection_does_not_skip_validation() -> Result<()> {
    let (root, source) = joined_fixture().await;
    let missing = ReadRequest::new("2026-06-30");
    assert_eq!(
        source
            .plan_scan(missing.clone())
            .await?
            .partitions
            .size_hint()
            .0,
        1
    );
    let metadata = source
        .metadata(missing.clone().with_projection(["court", "opinion"]))
        .await?;
    assert_eq!(metadata.schema.field(0).name(), "court");
    assert_eq!(metadata.schema.field(1).name(), "opinion");
    assert!(
        source
            .metadata(ReadRequest::new("2026-02-30"))
            .await
            .is_err()
    );
    assert!(
        source
            .metadata(missing.clone().with_projection(["unknown"]))
            .await
            .is_err()
    );
    assert!(source.open(missing).await.is_err());
    let request = ReadRequest::new("2026-09-30")
        .with_batch_size(NonZeroUsize::MIN)
        .with_projection(["court", "opinion"]);
    let plan = source.plan_scan(request.clone()).await?;
    assert_eq!(plan.partitions.size_hint().0, 1);
    let mut reader = source.open(request).await?;
    let batch = reader.next().await?.unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.schema().field(0).name(), "court");
    drop(reader);
    assert_eq!(
        consume(
            source
                .open(ReadRequest::new("2026-09-30").with_projection(Vec::<String>::new()))
                .await?
        )
        .await?,
        5
    );
    write_bz(
        root.path(),
        "courts",
        &JOIN_FIXTURES[3].1.replace("\"1.5\"", "\"NaN\""),
    )
    .await;
    assert!(
        source
            .open(ReadRequest::new("2026-09-30").with_projection(["opinion"]))
            .await
            .is_err()
    );
    tokio::fs::remove_file(root.path().join("bulk-data/courts-2026-09-30.csv.bz2"))
        .await
        .unwrap();
    assert!(source.open(ReadRequest::new("2026-09-30")).await.is_err());
    assert!(
        source
            .open(ReadRequest::new("../2026-09-30"))
            .await
            .is_err()
    );
    assert!(
        source
            .open(ReadRequest::new("2026-09-30").with_projection(["unknown"]))
            .await
            .is_err()
    );
    Ok(())
}
#[tokio::test]
async fn duplicate_ids_header_drift_bad_counts_and_empty_exports_fail() {
    for (table, raw) in JOIN_FIXTURES {
        let (root, source) = joined_fixture().await;
        let duplicate = raw.split_once('\n').unwrap().1;
        write_bz(root.path(), table, &format!("{raw}{duplicate}")).await;
        assert!(
            source.open(ReadRequest::new("2026-09-30")).await.is_err(),
            "duplicate IDs in {table} must fail before returning a reader"
        );
    }
    for raw in [
        JOIN_FIXTURES[0].1.replace("\"12\"", "\"-1\""),
        JOIN_FIXTURES[0].1.replacen("id,", "unknown,", 1),
        String::new(),
    ] {
        let (root, source) = joined_fixture().await;
        write_bz(root.path(), "opinions", &raw).await;
        assert!(source.open(ReadRequest::new("2026-09-30")).await.is_err());
    }
}

#[tokio::test]
async fn joined_output_preserves_input_order_and_ordinals_across_batches() -> Result<()> {
    let (root, source) = joined_fixture().await;
    let (header, _) = JOIN_FIXTURES[0].1.split_once('\n').unwrap();
    let row = JOIN_FIXTURES[0].1.lines().last().unwrap();
    let mut raw = format!("{header}\n");
    // Cross the typed reader's 1024-row boundary; IDs deliberately oppose input order.
    let rows = 2051;
    for id in (1..=rows).rev() {
        raw.push_str(&row.replacen("\"5\",", &format!("\"{id}\","), 1));
        raw.push('\n');
    }
    write_bz(root.path(), "opinions", &raw).await;
    let mut reader = source
        .open(
            ReadRequest::new("2026-09-30")
                .with_projection(["provenance", "opinion"])
                .with_batch_size(NonZeroUsize::new(128).unwrap()),
        )
        .await?;
    let mut seen = 0;
    while let Some(batch) = reader.next().await? {
        assert!(batch.num_rows() > 0 && batch.num_rows() <= 128);
        assert_eq!(batch.schema(), reader.metadata().schema);
        let provenance = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let opinion = batch
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        for index in 0..batch.num_rows() {
            assert_eq!(
                child::<UInt64Array>(provenance, "opinion_record_index").value(index),
                seen as u64
            );
            assert_eq!(
                child::<Int64Array>(opinion, "id").value(index),
                (rows - seen) as i64
            );
            seen += 1;
        }
    }
    assert_eq!(seen, rows);
    assert!(reader.next().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn header_only_opinions_produce_empty_joined_stream() -> Result<()> {
    let (root, source) = joined_fixture().await;
    let header = JOIN_FIXTURES[0].1.split_once('\n').unwrap().0;
    write_bz(root.path(), "opinions", &format!("{header}\n")).await;
    for projection in [vec!["opinion"], vec![]] {
        let request = ReadRequest::new("2026-09-30").with_projection(projection);
        let metadata = source.metadata(request.clone()).await?;
        let mut reader = source.open(request).await?;
        assert_eq!(reader.metadata().schema, metadata.schema);
        assert!(reader.next().await?.is_none());
        assert!(reader.next().await?.is_none());
    }
    Ok(())
}
