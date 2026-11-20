use std::{num::NonZeroUsize, sync::Arc};

use arrow_array::{LargeBinaryArray, cast::AsArray};
use futures::{StreamExt, TryStreamExt};
use open_arrow_access_core::{
    Accessor, AccessorExt, BinaryDecoder, ByteAccessor, ByteReadRequest, ByteReader, Capability,
    DocumentAccessor, ErrorKind, ListRequest, ObjectStream, ReadOptions, Result, ScanPlanner,
    async_trait,
};
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_service::{
    FileSetAccessor, FileSetRequest,
    bhl::{BhlOpenData, OPEN_DATA_BUCKET_URL, open_data_bucket_operator},
};
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{expect_err_kind, temp_fs};
use tokio::sync::Barrier;

const FIRST: &str = "ocr/item-346951/item-346951-65077706-0014.txt";
const SECOND: &str = "ocr/part-098691/part-098691-64253797-0001.txt";
const OTHER: &str = "ocr/item-021356/item-021356-02839616-0127.txt";

#[derive(Clone)]
struct Tracked {
    inner: OpenDalAccessor,
    opened: Arc<std::sync::Mutex<Vec<ByteReadRequest>>>,
    listed: Arc<std::sync::Mutex<Vec<String>>>,
    barrier: Option<Arc<Barrier>>,
}

#[async_trait]
impl Accessor for Tracked {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;
    fn capability(&self) -> Capability {
        self.inner.capability()
    }
    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.opened.lock().unwrap().push(request.clone());
        if let Some(barrier) = &self.barrier {
            barrier.wait().await;
        }
        self.inner.open(request).await
    }
}

#[async_trait]
impl ByteAccessor for Tracked {
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        self.listed
            .lock()
            .unwrap()
            .push(request.prefix.locator().to_owned());
        self.inner.list(request).await
    }
}

fn tracked(operator: opendal::Operator) -> Tracked {
    Tracked {
        inner: OpenDalAccessor::new(operator),
        opened: Arc::default(),
        listed: Arc::default(),
        barrier: None,
    }
}

#[tokio::test]
async fn discovers_directory_members_but_explicit_partitions_never_list_or_read_others() {
    let (_root, operator) = temp_fs(&[
        (FIRST, b"\r\n"),
        (SECOND, b"part OCR"),
        (OTHER, b"not selected"),
        ("images/CAT109916943238207426/page.jp2", b"image bytes"),
        ("scandata/CAT109916943238207426_scandata.xml", b"<book/>"),
        ("data/title.txt.gz", b"compressed metadata"),
    ]);
    let storage = tracked(operator);
    let source = BhlOpenData::new(storage.clone());
    assert_eq!(
        source
            .discover(ListRequest::new("ocr/item-346951/"))
            .await
            .unwrap(),
        vec![FIRST.into()]
    );
    assert!(storage.opened.lock().unwrap().is_empty());
    assert!(
        source
            .discover(ListRequest::new("ocr/").with_recursive(false))
            .await
            .unwrap()
            .is_empty()
    );
    let listed = storage.listed.lock().unwrap().len();
    let keys = [
        FIRST,
        SECOND,
        "images/CAT109916943238207426/page.jp2",
        "scandata/CAT109916943238207426_scandata.xml",
        "data/title.txt.gz",
    ];
    let files = source.partition(keys).unwrap();
    assert_eq!(files.metadata().len(), keys.len());
    let cached = files
        .list(ListRequest::new(""))
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(cached, keys.map(Into::into));
    let records =
        FileSetAccessor::new(DocumentAccessor::new(files, BinaryDecoder)).with_file_planning();
    let mut plan = records.plan_scan(FileSetRequest::new(keys)).await.unwrap();
    assert!(storage.opened.lock().unwrap().is_empty());
    assert_eq!(storage.listed.lock().unwrap().len(), listed);
    let mut count = 0;
    while let Some(partition) = plan.partitions.next().await {
        let mut reader = partition.unwrap().await.unwrap();
        assert!(storage.opened.lock().unwrap().len() == count);
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(reader.next().await.unwrap().is_none());
        count += 1;
    }
    assert_eq!(count, keys.len());
    let opened = storage.opened.lock().unwrap();
    assert_eq!(
        opened
            .iter()
            .map(|r| r.object.locator())
            .collect::<Vec<_>>(),
        keys
    );
    assert!(!opened.iter().any(|r| r.object.locator() == OTHER));
}

#[tokio::test]
async fn independent_page_partitions_execute_concurrently_with_exact_bytes_and_options() {
    let (_root, operator) = temp_fs(&[(FIRST, b"\r\n"), (SECOND, &[0xff, 0, 1])]);
    let mut storage = tracked(operator);
    // A serial reader would deadlock: both independently opened page objects
    // must enter the shared executor before either can complete its open.
    storage.barrier = Some(Arc::new(Barrier::new(2)));
    let files = BhlOpenData::new(storage.clone())
        .partition([FIRST, SECOND])
        .unwrap();
    let records = FileSetAccessor::new(DocumentAccessor::new(files, BinaryDecoder))
        .with_file_planning()
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::MIN,
        ));
    let byte_options = open_arrow_access_core::ByteReadOptions {
        io_chunk_size: NonZeroUsize::new(2),
        ..Default::default()
    };
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut reader = records
            .open(FileSetRequest::new([FIRST, SECOND]).with_byte_options(byte_options))
            .await
            .unwrap();
        let mut values = Vec::new();
        while let Some(batch) = reader.next().await.unwrap() {
            let data: &LargeBinaryArray = batch["data"].as_binary::<i64>();
            values.push(data.value(0).to_vec());
        }
        values
    })
    .await
    .expect("independent page partitions must overlap");
    assert_eq!(result, vec![b"\r\n".to_vec(), vec![0xff, 0, 1]]);
    let opened = storage.opened.lock().unwrap();
    assert_eq!(opened.len(), 2);
    assert!(opened.iter().all(|r| r.options == byte_options));
    assert!(storage.listed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn invalid_selection_and_unknown_names_cannot_escape_partition_membership() {
    let (_root, operator) = temp_fs(&[(FIRST, b"OCR")]);
    let storage = tracked(operator);
    let source = BhlOpenData::new(storage.clone());
    for keys in [
        vec![],
        vec![FIRST, FIRST],
        vec!["s3://bhl-open-data/ocr/page.txt"],
        vec!["ocr/../data/title.txt.gz"],
        vec!["ocr/item/"],
        vec!["/ocr/item/page.txt"],
        vec!["unknown/page.txt"],
    ] {
        expect_err_kind(source.partition(keys), ErrorKind::InvalidInput);
    }
    let files = source.partition([FIRST]).unwrap();
    expect_err_kind(files.open(OTHER.into()).await, ErrorKind::NotFound);
    assert!(storage.opened.lock().unwrap().is_empty());
    assert!(storage.listed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn projection_and_missing_object_preserve_file_set_error_contract() {
    let (_root, operator) = temp_fs(&[(FIRST, b"")]);
    let source = BhlOpenData::new(OpenDalAccessor::new(operator));
    let files = source.partition([FIRST, SECOND]).unwrap();
    let records =
        FileSetAccessor::new(DocumentAccessor::new(files, BinaryDecoder)).with_file_planning();
    let mut reader = records
        .open(
            FileSetRequest::new([FIRST, SECOND]).with_options(ReadOptions {
                projection: Some(vec![]),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 0);
    expect_err_kind(reader.next().await, ErrorKind::NotFound);
}

#[test]
fn public_bucket_operator_is_listable_s3() {
    assert_eq!(OPEN_DATA_BUCKET_URL, "s3://bhl-open-data/");
    let operator = open_data_bucket_operator().unwrap();
    assert_eq!(operator.info().scheme(), "s3");
    assert!(operator.info().capability().list);
}
