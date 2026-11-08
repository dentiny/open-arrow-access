use arrow_array::cast::AsArray;
use flate2::{Compression, write::GzEncoder};
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadOptions, ByteReadRequest, ByteReader, Capability, Error,
    ErrorKind, ReadOptions, ReadRequest, Result, async_trait,
};
use open_arrow_access_service::pmc_inventory::PmcInventoryAccessor;
use open_arrow_access_test_support::MemoryAccessor;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    io::Write,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
const MANIFEST: &str = "snapshot/manifest.json";
const FIRST: &str = "data/first.csv.gz";
const SECOND: &str = "data/second.csv.gz";
const ROW: &str = "pmc-oa-opendata,metadata%2FPMC1.1.json,2026-11-02,tag\n";
fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut e = GzEncoder::new(Vec::new(), Compression::default());
    e.write_all(bytes).unwrap();
    e.finish().unwrap()
}
#[derive(Clone, Default)]
struct Objects {
    files: HashMap<String, MemoryAccessor>,
    opened: Arc<Mutex<Vec<ByteReadRequest>>>,
}
impl Objects {
    fn insert(&mut self, key: &str, bytes: &[u8]) {
        self.files.insert(key.into(), MemoryAccessor::new(bytes, 1));
    }
    fn manifest(&mut self, value: &Value) {
        self.insert(MANIFEST, &serde_json::to_vec(value).unwrap());
    }
    fn fixture() -> (Self, Value) {
        let mut s = Self::default();
        s.insert(FIRST, &gzip(format!("{ROW}{ROW}").as_bytes()));
        s.insert(SECOND, &gzip(ROW.as_bytes()));
        let m = json!({"sourceBucket":"pmc-oa-opendata","destinationBucket":"arn:aws:s3:::pmc-oa-opendata","version":"2016-11-30","creationTimestamp":"1793581200000","fileFormat":"CSV","fileSchema":"Bucket, Key, LastModifiedDate, ETag","future":"preserve","files":[{"key":FIRST},{"key":SECOND}]});
        s.manifest(&m);
        (s, m)
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
            .ok_or_else(|| {
                Error::new(ErrorKind::NotFound, "fixture", "missing")
                    .with_source(std::io::Error::from(std::io::ErrorKind::NotFound))
            })?
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
    let (source, _) = Objects::fixture();
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        io_chunk_size: NonZeroUsize::new(7),
    };
    let mut reader = PmcInventoryAccessor::new(source.clone())
        .open(request(None).with_byte_options(options))
        .await
        .unwrap();
    assert_eq!(source.opened.lock().unwrap().len(), 1);
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        assert!(Arc::ptr_eq(&batch.schema(), &reader.metadata().schema));
        assert_eq!(
            batch["Key"].as_string::<i32>().value(0),
            "metadata%2FPMC1.1.json"
        );
        rows += batch.num_rows();
    }
    assert_eq!(rows, 3);
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
    for names in [vec!["ETag", "Key"], vec![]] {
        let (source, _) = Objects::fixture();
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
async fn manifest_field_order_and_quoted_extra_fields_are_preserved() {
    let (mut source, mut manifest) = Objects::fixture();
    manifest["fileSchema"] = json!("ETag,Key,Bucket,LastModifiedDate,FutureField");
    manifest["files"].as_array_mut().unwrap().truncate(1);
    source.manifest(&manifest);
    source.insert(
        FIRST,
        &gzip(b"tag,metadata/PMC1.1.json,pmc-oa-opendata,date,\"quoted,extra\"\n"),
    );
    let mut reader = PmcInventoryAccessor::new(source)
        .open(request(None))
        .await
        .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.schema().field(0).name(), "ETag");
    assert_eq!(
        batch["FutureField"].as_string::<i32>().value(0),
        "quoted,extra"
    );
}

#[tokio::test]
async fn invalid_manifests_fail_before_opening_shards() {
    for (field, value) in [
        ("sourceBucket", json!("other")),
        ("creationTimestamp", json!("yesterday")),
        ("fileSchema", json!("Bucket,Key,Key,ETag")),
        ("files", json!([{"key":"../escape.csv.gz"}])),
    ] {
        let (mut source, mut manifest) = Objects::fixture();
        manifest[field] = value;
        source.manifest(&manifest);
        let error = PmcInventoryAccessor::new(source.clone())
            .open(request(None))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::Decode, "{field}");
        assert_eq!(source.opened.lock().unwrap().len(), 1);
    }
    let (source, _) = Objects::fixture();
    assert_eq!(
        PmcInventoryAccessor::new(source)
            .with_manifest_limit(NonZeroUsize::MIN)
            .open(request(None))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Decode
    );
}

#[tokio::test]
async fn validates_rows_even_with_zero_column_projection() {
    for row in [
        "other,metadata/a.json,date,tag\n",
        "pmc-oa-opendata,,date,tag\n",
    ] {
        let (mut source, _) = Objects::fixture();
        source.insert(FIRST, &gzip(row.as_bytes()));
        let mut reader = PmcInventoryAccessor::new(source)
            .open(request(Some(vec![])))
            .await
            .unwrap();
        assert_eq!(
            reader.next().await.unwrap_err().kind(),
            ErrorKind::Decode,
            "{row}"
        );
    }
}

#[tokio::test]
async fn empty_manifests_and_shards_keep_the_declared_schema() {
    let (mut source, mut manifest) = Objects::fixture();
    manifest["files"] = json!([]);
    source.manifest(&manifest);
    let mut reader = PmcInventoryAccessor::new(source)
        .open(request(None))
        .await
        .unwrap();
    assert_eq!(reader.metadata().schema.fields().len(), 4);
    assert!(reader.next().await.unwrap().is_none());
    let (mut source, _) = Objects::fixture();
    source.insert(FIRST, &gzip(b""));
    let mut reader = PmcInventoryAccessor::new(source)
        .open(request(None))
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
    assert!(reader.next().await.unwrap().is_none());
}
