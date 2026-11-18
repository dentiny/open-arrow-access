use std::sync::{Arc, Mutex};

use open_arrow_access_core::{
    Accessor, Error, ErrorKind, OrderRequirement, ReadRequest, RecordCapability, RecordReader,
    Result, ScanStatistics, Statistic, StatisticsProvider, async_trait,
};
use open_arrow_access_dataset::directory::{DirectoryAccessor, DirectoryRequest};
use open_arrow_access_dataset::file_set::{FileSetAccessor, FileSetRequest};
use open_arrow_access_file_format::parquet::ParquetAccessor;
use open_arrow_access_test_support::{MemoryAccessor, fixture_path};

#[derive(Clone, Default)]
struct MetadataOnly(Arc<Mutex<Vec<ReadRequest>>>);

#[async_trait]
impl Accessor for MetadataOnly {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    fn capability(&self) -> RecordCapability {
        RecordCapability::sequential(OrderRequirement::Reorderable)
    }
    async fn open(&self, _: ReadRequest) -> Result<Box<dyn RecordReader>> {
        panic!("statistics must not open record readers")
    }
}

#[async_trait]
impl StatisticsProvider for MetadataOnly {
    async fn statistics(&self, request: ReadRequest) -> Result<ScanStatistics> {
        self.0.lock().unwrap().push(request.clone());
        let bytes = match request.object.locator() {
            "exact" => Statistic::Exact(10),
            "estimate" => Statistic::Estimated(20),
            "unknown" => Statistic::Unknown,
            "large" => Statistic::Exact(u64::MAX),
            _ => {
                return Err(Error::new(
                    ErrorKind::Io,
                    "test_statistics",
                    "metadata error",
                ));
            }
        };
        Ok(ScanStatistics {
            row_count: Statistic::Exact(1),
            source_bytes: bytes,
        })
    }
}

#[tokio::test]
async fn sums_accuracy_duplicates_and_options_without_reading() {
    let provider = MetadataOnly::default();
    let files = FileSetAccessor::new(provider.clone());
    let mut request = FileSetRequest::new(["exact", "estimate", "exact"]);
    request.options.projection = Some(vec!["id".into()]);
    request.byte_options.io_concurrency = std::num::NonZeroUsize::new(3).unwrap();
    let stats = files.statistics(request.clone()).await.unwrap();
    assert_eq!(stats.row_count, Statistic::Exact(3));
    assert_eq!(stats.source_bytes, Statistic::Estimated(40));
    for child in provider.0.lock().unwrap().iter() {
        assert_eq!(child.options.projection, request.options.projection);
        assert_eq!(
            child.byte_options.io_concurrency,
            request.byte_options.io_concurrency
        );
    }
    assert_eq!(
        files
            .statistics(FileSetRequest::new(["exact", "exact"]))
            .await
            .unwrap()
            .source_bytes,
        Statistic::Exact(20)
    );
    assert_eq!(
        files
            .statistics(FileSetRequest::new(["exact", "unknown"]))
            .await
            .unwrap()
            .source_bytes,
        Statistic::Unknown
    );
    assert_eq!(
        files
            .statistics(FileSetRequest::new(["large", "exact"]))
            .await
            .unwrap()
            .source_bytes,
        Statistic::Unknown
    );
    assert!(
        files
            .statistics(FileSetRequest::new(["fail"]))
            .await
            .is_err()
    );
    assert!(
        files
            .statistics(FileSetRequest::new(Vec::<String>::new()))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn union_statistics_do_not_project_absent_child_columns() {
    let provider = MetadataOnly::default();
    let files = FileSetAccessor::new(provider.clone()).with_union_by_name();
    let mut request = FileSetRequest::new(["exact", "estimate"]);
    request.options.projection = Some(vec!["only_in_one_file".into()]);
    files.statistics(request).await.unwrap();
    assert!(
        provider
            .0
            .lock()
            .unwrap()
            .iter()
            .all(|r| r.options.projection.is_none())
    );
}

#[tokio::test]
async fn directory_statistics_use_the_same_glob_and_suffix_selection() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("sub")).unwrap();
    let data = std::fs::read(fixture_path!("parquet", "ordered-uncompressed.parquet")).unwrap();
    for name in ["a.parquet", "sub/b.parquet", "ignore.txt"] {
        std::fs::write(dir.path().join(name), &data).unwrap();
    }
    let fs = open_arrow_access_storage::OpenDalAccessor::new(
        open_arrow_access_test_support::fs_operator(dir.path()),
    );
    let files = DirectoryAccessor::new(fs.clone(), ParquetAccessor::new(fs));
    let mut request = DirectoryRequest::new("**/*.parquet");
    request.suffix = Some(".parquet".into());
    let stats = files.statistics(request.clone()).await.unwrap();
    assert_eq!(stats.row_count, Statistic::Exact(14));
    assert_eq!(stats.source_bytes, Statistic::Exact(2 * data.len() as u64));
    request.recursive = false;
    assert_eq!(
        files.statistics(request).await.unwrap().row_count,
        Statistic::Exact(7)
    );
}

#[tokio::test]
async fn prepared_parquet_retains_metadata_before_streaming_records() {
    let data = std::fs::read(fixture_path!("parquet", "ordered-uncompressed.parquet")).unwrap();
    let source = open_arrow_access_core::PreparedSource::prepare_plannable_with_statistics(
        ParquetAccessor::new(MemoryAccessor::new(&data, 64).with_ranges()),
        ReadRequest::new("data.parquet"),
    )
    .await
    .unwrap();
    let stats = source.statistics(Default::default()).await.unwrap();
    assert_eq!(stats.row_count, Statistic::Exact(7));
    let mut reader = source.scan(Default::default()).await.unwrap();
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        rows += batch.num_rows();
    }
    assert_eq!(rows, 7);
}
