use std::sync::{Arc, Mutex};

use futures::TryStreamExt;
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, ListRequest, ObjectStream,
    Result, async_trait,
};
use open_arrow_access_service::bhl::BhlOpenData;
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{byte_accessor, temp_fs};

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
async fn inventory_jobs_preserve_scope_and_workers_need_no_listing() {
    use open_arrow_access_service::bhl::{BhlInventory, BhlOcrScope, BhlPageJob};
    use open_arrow_access_test_support::fixture;
    use std::num::NonZeroUsize;

    const PART: &str = "ocr/part-098691/part-098691-65077706-0001.txt";
    let (_root, operator) = temp_fs(&[
        (FIRST, b"item OCR"),
        (OTHER, b"other item"),
        (PART, b"part OCR"),
        ("images/book/page.jp2", b"not OCR"),
    ]);
    let storage = Tracked::new(operator);
    let source = BhlOpenData::new(storage.clone());
    let inventory = source
        .inventory("fixture-snapshot", BhlOcrScope::All)
        .await
        .unwrap();
    let saved =
        BhlInventory::read_json(fixture!("bhl-inventory", "inventory.json").await.as_slice())
            .unwrap();
    assert_eq!(inventory.objects(), saved.objects());
    assert_eq!(*storage.listed.lock().unwrap(), ["ocr/"]);
    assert!(storage.opened.lock().unwrap().is_empty());
    for (scope, count) in [(BhlOcrScope::Items, 2), (BhlOcrScope::Parts, 1)] {
        assert_eq!(
            source
                .inventory("fixture-snapshot", scope)
                .await
                .unwrap()
                .objects()
                .len(),
            count
        );
    }
    let mut all_keys = Vec::new();
    for (index, job) in saved.jobs(NonZeroUsize::new(2).unwrap()).enumerate() {
        assert_eq!(job.index(), index);
        assert_eq!(job.snapshot_id(), "fixture-snapshot");
        assert!(job.objects().len() <= 2);
        let mut bytes = Vec::new();
        job.write_json(&mut bytes).unwrap();
        let loaded = BhlPageJob::read_json(bytes.as_slice()).unwrap();
        assert_eq!(loaded.objects(), job.objects());
        all_keys.extend_from_slice(loaded.objects());
    }
    assert_eq!(all_keys, saved.objects());
    let job =
        BhlPageJob::read_json(fixture!("bhl-inventory", "job.json").await.as_slice()).unwrap();
    assert_eq!(job.index(), 1);
    assert_eq!(
        job.request().objects,
        vec![open_arrow_access_core::ObjectRef::new(PART)]
    );
    let listed = storage.listed.lock().unwrap().len();
    let files = source.partition(job.objects().iter().cloned()).unwrap();
    byte_accessor::assert_read_contract(files, PART.into(), b"part OCR")
        .await
        .unwrap();
    assert_eq!(storage.listed.lock().unwrap().len(), listed);
    assert!(storage.opened.lock().unwrap().iter().all(|key| key == PART));
}

#[tokio::test]
async fn saved_inventory_and_job_validation_rejects_inconsistent_work() {
    use open_arrow_access_service::bhl::{BhlInventory, BhlPageJob};
    use open_arrow_access_test_support::fixture;

    for file in [
        "bad-version.json",
        "duplicate-identity.json",
        "unsorted.json",
        "wrong-scope.json",
        "unknown-field.json",
    ] {
        assert!(
            BhlInventory::read_json(fixture!("bhl-inventory", file).await.as_slice()).is_err(),
            "{file}"
        );
    }
    for file in [
        "bad-job-index.json",
        "bad-job-size.json",
        "bad-job-overflow.json",
    ] {
        assert!(
            BhlPageJob::read_json(fixture!("bhl-inventory", file).await.as_slice()).is_err(),
            "{file}"
        );
    }
}
