use std::sync::{Arc, Mutex};

use futures::TryStreamExt;
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, ErrorKind, ListRequest,
    ObjectStream, Result, async_trait,
};
use open_arrow_access_service::bhl::BhlOpenData;
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{byte_accessor, expect_err_kind, temp_fs};

const FIRST: &str = "ocr/item-346951/item-346951-65077706-0014.txt";
const SECOND: &str = "ocr/part-098691/part-098691-64253797-0001.txt";
const OTHER: &str = "ocr/item-021356/item-021356-02839616-0127.txt";

#[derive(Clone)]
struct Tracked {
    inner: OpenDalAccessor,
    opened: Arc<Mutex<Vec<String>>>,
    listed: Arc<Mutex<Vec<String>>>,
}

impl Tracked {
    fn new(operator: opendal::Operator) -> Self {
        Self {
            inner: OpenDalAccessor::new(operator),
            opened: Arc::default(),
            listed: Arc::default(),
        }
    }
}

#[async_trait]
impl Accessor for Tracked {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.opened
            .lock()
            .unwrap()
            .push(request.object.locator().to_owned());
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

#[tokio::test]
async fn discovery_is_scoped_and_explicit_selection_performs_no_io() {
    let keys = [
        FIRST,
        SECOND,
        "images/book/page.jp2",
        "scandata/book.xml",
        "data/title.txt.gz",
    ];
    let (_root, operator) = temp_fs(&[
        (FIRST, b"OCR"),
        (SECOND, b"part OCR"),
        (OTHER, b"unselected"),
    ]);
    let storage = Tracked::new(operator);
    let source = BhlOpenData::new(storage.clone());
    assert_eq!(
        source
            .discover(ListRequest::new("ocr/item-346951/"))
            .await
            .unwrap(),
        vec![FIRST.into()]
    );
    assert!(
        source
            .discover(ListRequest::new("ocr/").with_recursive(false))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        *storage.listed.lock().unwrap(),
        ["ocr/item-346951/", "ocr/"]
    );
    assert!(storage.opened.lock().unwrap().is_empty());

    let files = source.partition(keys).unwrap();
    assert_eq!(files.metadata(), &keys.map(Into::into).to_vec());
    let cached = files
        .list(ListRequest::new(""))
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(cached, keys.map(Into::into));
    assert_eq!(storage.listed.lock().unwrap().len(), 2);
    assert!(storage.opened.lock().unwrap().is_empty());

    byte_accessor::assert_read_contract(files, FIRST.into(), b"OCR")
        .await
        .unwrap();
    assert!(
        storage
            .opened
            .lock()
            .unwrap()
            .iter()
            .all(|key| key == FIRST)
    );
    assert_eq!(storage.listed.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn invalid_keys_and_unselected_objects_fail_before_storage_io() {
    let (_root, operator) = temp_fs(&[(FIRST, b"OCR")]);
    let storage = Tracked::new(operator);
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
    for prefix in ["unknown/", "ocr/../", "ocr/item?query/"] {
        expect_err_kind(
            source.discover(ListRequest::new(prefix)).await,
            ErrorKind::InvalidInput,
        );
    }
    let files = source.partition([FIRST]).unwrap();
    expect_err_kind(files.open(OTHER.into()).await, ErrorKind::NotFound);
    assert!(storage.opened.lock().unwrap().is_empty());
    assert!(storage.listed.lock().unwrap().is_empty());
}
