use arrow_array::cast::AsArray;
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadOptions, ByteReadRequest, ByteReader, Capability, Error,
    ErrorKind, ReadOptions, ReadRequest, Result, async_trait,
};
use open_arrow_access_service::pmc_inventory::PmcInventoryAccessor;
use open_arrow_access_test_support::{MemoryAccessor, fixture};
use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
const MANIFEST: &str = "s3://pmc-oa-opendata/snapshot/inventory.json";
const FIRST: &str = "data/first.csv.gz";
const SECOND: &str = "data/second.csv.gz";
#[derive(Clone, Default)]
struct Objects {
    files: HashMap<String, MemoryAccessor>,
    opened: Arc<Mutex<Vec<ByteReadRequest>>>,
}
impl Objects {
    fn insert(&mut self, key: &str, bytes: &[u8]) {
        self.files.insert(key.into(), MemoryAccessor::new(bytes, 1));
    }
    async fn fixture() -> Self {
        let mut source = Self::default();
        source.insert(FIRST, &fixture!("pmc/inventory", "first.csv.gz").await);
        source.insert(SECOND, &fixture!("pmc/inventory", "second.csv.gz").await);
        source.insert(MANIFEST, &fixture!("pmc/inventory", "manifest.json").await);
        source
    }
}
#[async_trait]
impl Accessor for Objects {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }
    async fn open(&self, r: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.opened.lock().unwrap().push(r.clone());
        self.files
            .get(r.object.locator())
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "fixture", "missing"))?
            .open(r)
            .await
    }
}
#[async_trait]
impl ByteAccessor for Objects {}
fn request(p: Option<Vec<&str>>) -> ReadRequest {
    ReadRequest::new(MANIFEST).with_options(ReadOptions {
        batch_size: NonZeroUsize::MIN,
        projection: p.map(|v| v.into_iter().map(String::from).collect()),
    })
}
#[tokio::test]
async fn streams_shards_in_order_with_original_values_and_byte_options() {
    let source = Objects::fixture().await;
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        io_chunk_size: NonZeroUsize::new(7),
    };
    let mut reader = PmcInventoryAccessor::new(source.clone())
        .open(request(None).with_byte_options(options))
        .await
        .unwrap();
    assert_eq!(source.opened.lock().unwrap().len(), 1);
    assert_eq!(
        reader
            .metadata()
            .schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["ETag", "Key", "Bucket", "LastModifiedDate", "FutureField"]
    );
    let mut keys = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        assert!(Arc::ptr_eq(&batch.schema(), &reader.metadata().schema));
        assert_eq!(batch["FutureField"].as_string::<i32>().value(0), "extra");
        let column = batch["Key"].as_string::<i32>();
        keys.extend((0..batch.num_rows()).map(|row| column.value(row).to_owned()));
    }
    assert_eq!(
        keys,
        [
            "metadata%2FPMC1.1.json",
            "metadata%2FPMC1.1.json",
            "metadata%2FPMC2.1.json"
        ]
    );
    let opened = source.opened.lock().unwrap();
    assert_eq!(
        opened
            .iter()
            .map(|request| request.object.locator())
            .collect::<Vec<_>>(),
        [MANIFEST, FIRST, SECOND]
    );
    assert!(opened.iter().all(|request| request.options == options));
}

#[tokio::test]
async fn projection_preserves_output_order_and_zero_column_rows() {
    for names in [
        vec!["ETag", "Key"],
        vec!["Key", "Bucket"],
        vec!["ETag"],
        vec![],
    ] {
        let source = Objects::fixture().await;
        let mut reader = PmcInventoryAccessor::new(source)
            .open(request(Some(names.clone())))
            .await
            .unwrap();
        let mut rows = 0;
        while let Some(batch) = reader.next().await.unwrap() {
            assert_eq!(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<_>>(),
                names
            );
            rows += batch.num_rows();
        }
        assert_eq!(rows, 3);
    }
}

#[tokio::test]
async fn invalid_manifests_fail_before_opening_shards() {
    for name in [
        "bad_bucket.json",
        "bad_timestamp.json",
        "bad_schema.json",
        "bad_shard_path.json",
    ] {
        let mut source = Objects::fixture().await;
        source.insert(MANIFEST, &fixture!("pmc/inventory", name).await);
        let error = PmcInventoryAccessor::new(source.clone())
            .open(request(None))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::Decode, "{name}");
        assert_eq!(source.opened.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn validates_rows_even_with_zero_column_projection() {
    for name in ["bad_bucket.csv.gz", "bad_empty_key.csv.gz"] {
        let mut source = Objects::fixture().await;
        source.insert(FIRST, &fixture!("pmc/inventory", name).await);
        let mut reader = PmcInventoryAccessor::new(source)
            .open(request(Some(vec![])))
            .await
            .unwrap();
        assert_eq!(
            reader.next().await.unwrap_err().kind(),
            ErrorKind::Decode,
            "{name}"
        );
    }
}

#[tokio::test]
async fn empty_manifests_and_shards_keep_the_declared_schema() {
    let mut source = Objects::fixture().await;
    source.insert(
        MANIFEST,
        &fixture!("pmc/inventory", "empty_manifest.json").await,
    );
    let mut reader = PmcInventoryAccessor::new(source)
        .open(request(None))
        .await
        .unwrap();
    assert_eq!(reader.metadata().schema.fields().len(), 5);
    assert!(reader.next().await.unwrap().is_none());
    let mut source = Objects::fixture().await;
    source.insert(FIRST, &fixture!("pmc/inventory", "empty.csv.gz").await);
    let mut reader = PmcInventoryAccessor::new(source)
        .open(request(None))
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn manifest_metadata_can_exceed_the_previous_size_limit() {
    let mut source = Objects::fixture().await;
    let mut raw = fixture!("pmc/inventory", "manifest.json").await;
    raw.extend(std::iter::repeat_n(b' ', 4 * 1024 * 1024));
    source
        .files
        .insert(MANIFEST.into(), MemoryAccessor::new(&raw, 64 * 1024));
    assert!(
        PmcInventoryAccessor::new(source)
            .open(request(None))
            .await
            .is_ok()
    );
}
