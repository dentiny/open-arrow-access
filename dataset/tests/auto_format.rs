use arrow_array::{Int64Array, types::Int64Type};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use open_arrow_access_core::{
    Accessor, AccessorExt, ErrorKind, ReadOptions, ReadOptionsProvider, ScanPlanner,
};
use open_arrow_access_dataset::{
    auto_format::{AutoFormatAccessor, AutoFormatRequest, DetectedFormat},
    directory::DirectoryRequest,
    file_set::FileSetRequest,
};
use open_arrow_access_file_format::csv::{CsvDialect, CsvHeader};
use open_arrow_access_layers::{ColumnEvolution, ParallelReadLayer, SchemaEvolutionLayer};
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{fixture_path, fs_operator, record::collect_column};
use std::{io::Write, num::NonZeroUsize, sync::Arc};

fn files(names: &[&str]) -> AutoFormatRequest {
    AutoFormatRequest::Files(FileSetRequest::new(names.iter().copied()))
}
fn storage(root: &std::path::Path) -> OpenDalAccessor {
    OpenDalAccessor::new(fs_operator(root))
}

#[tokio::test]
async fn directory_routes_csv_and_forwards_options_through_layers() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("a.csv"), "id,name\n1,'first, item'\n").unwrap();
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gzip.write_all(b"id,name\n2,second\n").unwrap();
    std::fs::write(root.path().join("b.CSV.GZ"), gzip.finish().unwrap()).unwrap();
    std::fs::write(root.path().join("README.txt"), "ignore").unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let auto = AutoFormatAccessor::new(storage(root.path()))
        .with_schema(schema)
        .with_csv_dialect(CsvDialect {
            header: CsvHeader::Present,
            quote: b'\'',
            ..Default::default()
        });
    let mut request = AutoFormatRequest::Directory(DirectoryRequest::new("*"));
    *request.read_options_mut() = ReadOptions {
        batch_size: NonZeroUsize::MIN,
        projection: Some(vec!["next".into()]),
    };
    let records = auto
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::MIN,
        ))
        .layer(
            SchemaEvolutionLayer::try_new(vec![ColumnEvolution::add_column_sql("next", "id + 1")])
                .unwrap(),
        );
    let mut reader = records.open(request).await.unwrap();
    let mut values = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!((batch.num_columns(), batch.num_rows()), (1, 1));
        values.push(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
        );
    }
    assert_eq!(values, [2, 3]);
}

#[tokio::test]
async fn parquet_retains_native_planning_and_rejects_schema_overrides() {
    let root = fixture_path!("parquet", "");
    let len = std::fs::metadata(root.join("ordered-uncompressed.parquet"))
        .unwrap()
        .len();
    let bytes = storage(&root).with_content_lengths([("ordered-uncompressed.parquet".into(), len)]);
    let auto = AutoFormatAccessor::new(bytes);
    let error = auto
        .resolve(files(&["ordered-uncompressed.parquet", "metadata.json"]))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    let plan = auto
        .plan_scan(files(&["ordered-uncompressed.parquet"]))
        .await
        .unwrap();
    let partitions = plan.partitions.try_collect::<Vec<_>>().await.unwrap();
    assert!(partitions.len() > 1);
    let values = collect_column::<Int64Type>(
        auto.open(files(&["ordered-uncompressed.parquet"]))
            .await
            .unwrap(),
        0,
    )
    .await;
    assert!(!values.is_empty());
    let explicit = auto.with_schema(Arc::new(Schema::empty()));
    for name in ["missing.parquet", "missing.warc"] {
        assert_eq!(
            explicit.resolve(files(&[name])).await.unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
}

#[tokio::test]
async fn explicit_format_keeps_all_objects_and_opens_unknown_suffixes() {
    let root = tempfile::tempdir().unwrap();
    for (format, content, other_suffix) in [
        (DetectedFormat::Csv, "id,name\n1,first\n", "records.tsv"),
        (DetectedFormat::Tsv, "id\tname\n1\tfirst\n", "records.csv"),
    ] {
        let names = ["records.data", other_suffix, "records.data"];
        for name in names {
            std::fs::write(root.path().join(name), content).unwrap();
        }
        let auto = AutoFormatAccessor::new(storage(root.path())).with_format(format);
        let selected = auto.resolve(files(&names)).await.unwrap();
        assert_eq!(selected.objects, FileSetRequest::new(names).objects);
        let values = collect_column::<Int64Type>(auto.open(files(&names)).await.unwrap(), 0).await;
        assert_eq!(values, [1, 1, 1]);
        assert_eq!(
            auto.resolve(files(&[])).await.unwrap_err().kind(),
            ErrorKind::NotFound
        );
    }
}

#[tokio::test]
async fn automatic_selection_rejects_mixed_csv_tsv_before_opening_files() {
    let root = tempfile::tempdir().unwrap();
    let auto = AutoFormatAccessor::new(storage(root.path()));
    let error = auto
        .resolve(files(&["missing.csv", "missing.tsv"]))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("missing.csv"));
    assert!(error.to_string().contains("missing.tsv"));
}
