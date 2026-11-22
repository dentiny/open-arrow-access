use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use arrow_array::cast::AsArray;
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadOptions, ByteReadRequest, ByteReader, Capability, ErrorKind,
    ListRequest, ObjectStream, ReadOptions, Result, async_trait,
};
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::temp_fs;

use super::{BhlShardAccessor, BhlShardReadRequest};
use crate::bhl::metadata::tests::load;

#[derive(Clone)]
struct Tracked {
    inner: OpenDalAccessor,
    listed: Arc<Mutex<Vec<String>>>,
    opened: Arc<Mutex<Vec<ByteReadRequest>>>,
}

impl Tracked {
    fn new(operator: opendal::Operator) -> Self {
        Self {
            inner: OpenDalAccessor::new(operator),
            listed: Arc::default(),
            opened: Arc::default(),
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
        self.opened.lock().unwrap().push(request.clone());
        self.inner.open_sequential(request).await
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

#[test]
fn requires_one_positive_item_or_part_directory() {
    for prefix in [
        "ocr/",
        "ocr/item-0/",
        "ocr/item-2",
        "ocr/item-2//",
        "ocr/item-2/nested/",
        "ocr/part-+8/",
        "images/item-2/",
        "ocr/item-18446744073709551616/",
    ] {
        assert!(BhlShardReadRequest::new(prefix).is_err(), "{prefix}");
    }
    assert_eq!(
        BhlShardReadRequest::new("ocr/part-0008/").unwrap().prefix(),
        "ocr/part-0008/"
    );
}

#[tokio::test]
async fn scoped_discovery_preserves_lazy_projection_metadata_and_byte_options() {
    let key = "ocr/item-0002/item-0002-0003-0005.txt";
    let (_root, operator) = temp_fs(&[
        (key, b"\r\n\xff"),
        ("ocr/item-0002/item-0002.txt", b"aggregate"),
        ("ocr/part-8/part-8-3-1.txt", b"other shard"),
    ]);
    let storage = Tracked::new(operator);
    let metadata = Arc::new(load().await);
    let fingerprint = metadata.info.fingerprint.clone();
    let source = BhlShardAccessor::from_metadata(storage.clone(), metadata).unwrap();
    let request = BhlShardReadRequest::new("ocr/item-0002/")
        .unwrap()
        .with_options(ReadOptions::default().with_projection(["page_id", "metadata"]));
    let expected = source.metadata(request.clone()).await.unwrap();
    assert_eq!(
        expected.schema.metadata()["bhl.metadata.fingerprint"],
        fingerprint
    );
    assert!(storage.listed.lock().unwrap().is_empty());
    assert!(storage.opened.lock().unwrap().is_empty());
    let mut reader = source.open(request).await.unwrap();
    assert_eq!(reader.metadata().schema, expected.schema);
    assert!(storage.opened.lock().unwrap().is_empty());
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(reader.next().await.unwrap().is_none());
    assert!(storage.opened.lock().unwrap().is_empty());

    let byte_options = ByteReadOptions {
        io_chunk_size: NonZeroUsize::new(2),
        ..ByteReadOptions::default()
    };
    let mut reader = source
        .with_snapshot_id("inventory")
        .unwrap()
        .open(
            BhlShardReadRequest::new("ocr/item-0002/")
                .unwrap()
                .with_byte_options(byte_options),
        )
        .await
        .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch["data"].as_binary::<i64>().value(0), b"\r\n\xff");
    assert_eq!(
        batch["snapshot_id"].as_string::<i32>().value(0),
        "inventory"
    );
    assert!(reader.next().await.unwrap().is_none());
    let opened = storage.opened.lock().unwrap();
    assert_eq!(opened.len(), 1);
    assert_eq!(opened[0].object.locator(), key);
    assert_eq!(opened[0].options, byte_options);
    assert_eq!(
        *storage.listed.lock().unwrap(),
        ["ocr/item-0002/", "ocr/item-0002/"]
    );
}

#[tokio::test]
async fn rejects_empty_and_unexpected_objects_without_reading_payloads() {
    let (_root, operator) = temp_fs(&[
        ("ocr/item-2/item-2.txt", b"aggregate"),
        ("ocr/part-8/unexpected.txt", b"invalid"),
    ]);
    let storage = Tracked::new(operator);
    let source = BhlShardAccessor::from_metadata(storage.clone(), Arc::new(load().await)).unwrap();
    for prefix in ["ocr/item-2/", "ocr/part-8/", "ocr/item-99/"] {
        let error = source
            .open(BhlShardReadRequest::new(prefix).unwrap())
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
    assert!(storage.opened.lock().unwrap().is_empty());
}

#[tokio::test]
async fn strict_metadata_failures_can_be_reported_and_retained_explicitly() {
    let missing = "ocr/item-20/item-20-30-1.txt";
    let conflict = "ocr/item-2/item-2-3-6.txt";
    let (_root, operator) = temp_fs(&[(missing, b"missing"), (conflict, b"conflict")]);
    let storage = Tracked::new(operator);
    let source = BhlShardAccessor::from_metadata(storage.clone(), Arc::new(load().await)).unwrap();
    for (prefix, key) in [("ocr/item-20/", missing), ("ocr/item-2/", conflict)] {
        let mut reader = source
            .open(BhlShardReadRequest::new(prefix).unwrap())
            .await
            .unwrap();
        let error = reader.next().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.object().unwrap().locator(), key);
    }
    assert!(storage.opened.lock().unwrap().is_empty());
    let reports = Arc::new(Mutex::new(Vec::new()));
    let observed = reports.clone();
    let source = source.with_report(move |page| {
        observed
            .lock()
            .unwrap()
            .push((page.missing.clone(), page.conflicts.clone()));
        Ok(())
    });
    for prefix in ["ocr/item-20/", "ocr/item-2/"] {
        let mut reader = source
            .open(BhlShardReadRequest::new(prefix).unwrap())
            .await
            .unwrap();
        assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
        assert!(reader.next().await.unwrap().is_none());
    }
    let reports = reports.lock().unwrap();
    assert!(!reports[0].0.is_empty());
    assert!(!reports[1].1.is_empty());
    assert_eq!(storage.opened.lock().unwrap().len(), 2);
}
