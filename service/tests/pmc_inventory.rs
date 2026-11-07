use arrow_array::StringArray;
use flate2::{Compression, write::GzEncoder};
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteAccessor, ByteReadOptions, ByteReadRequest, ByteReader, Capability,
    Error, ErrorKind, ReadOptions, ReadRequest, Result, async_trait,
};
use open_arrow_access_decompression::{DecompressionLayer, Gzip};
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
        let m = json!({"sourceBucket":"pmc-oa-opendata","destinationBucket":"arn:aws:s3:::pmc-oa-opendata","version":"2016-11-30","creationTimestamp":"1793581200000","fileFormat":"CSV","fileSchema":"Bucket, Key, LastModifiedDate, ETag","future":"preserve","files":[{"key":FIRST,"size":1,"MD5checksum":"0123456789abcdef0123456789abcdef"},{"key":SECOND,"size":1,"MD5checksum":"abcdef0123456789abcdef0123456789"}]});
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
            ..Capability::default()
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
async fn streams_in_manifest_order_and_preserves_identity_without_article_reads() {
    let (s, m) = Objects::fixture();
    let io = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        io_chunk_size: NonZeroUsize::new(7),
    };
    let mut r = PmcInventoryAccessor::new(s.clone())
        .open(request(None).with_byte_options(io))
        .await
        .unwrap();
    assert_eq!(s.opened.lock().unwrap().len(), 1);
    let schema = r.metadata().schema.clone();
    assert_eq!(
        schema.metadata()["pmc.inventory.creation_timestamp"],
        "1793581200000"
    );
    assert_eq!(
        serde_json::from_str::<Value>(&schema.metadata()["pmc.inventory.raw_manifest_json"])
            .unwrap(),
        m
    );
    let mut rows = 0;
    while let Some(b) = r.next().await.unwrap() {
        assert_eq!(b.schema(), schema);
        assert_eq!(b.num_rows(), 1);
        assert_eq!(
            b.column_by_name("metadata_key")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "metadata/PMC1.1.json"
        );
        assert_eq!(
            b.column_by_name("Key")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "metadata%2FPMC1.1.json"
        );
        rows += 1;
        if rows == 1 {
            assert_eq!(s.opened.lock().unwrap().len(), 2);
        }
    }
    assert_eq!(rows, 3);
    assert!(r.next().await.unwrap().is_none());
    let opened = s.opened.lock().unwrap();
    assert_eq!(
        opened
            .iter()
            .map(|x| x.object.locator())
            .collect::<Vec<_>>(),
        [MANIFEST, FIRST, SECOND]
    );
    assert!(opened.iter().all(|x| x.options == io));
}
#[tokio::test]
async fn projection_keeps_row_counts_and_validates_before_csv_reads() {
    for fields in [vec!["metadata_key", "ETag"], vec!["ETag", "Key"], vec![]] {
        let (s, _) = Objects::fixture();
        let mut r = PmcInventoryAccessor::new(s)
            .open(request(Some(fields.clone())))
            .await
            .unwrap();
        let mut n = 0;
        while let Some(b) = r.next().await.unwrap() {
            n += b.num_rows();
            assert_eq!(b.num_columns(), fields.len());
            assert!(Arc::ptr_eq(&b.schema(), &r.metadata().schema));
        }
        assert_eq!(n, 3);
    }
    for fields in [vec!["unknown"], vec!["Key", "Key"]] {
        let (s, _) = Objects::fixture();
        assert_eq!(
            PmcInventoryAccessor::new(s.clone())
                .open(request(Some(fields)))
                .await
                .err()
                .unwrap()
                .kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(s.opened.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn batches_share_large_manifest_schema_across_projections_and_shards() {
    for fields in [None, Some(vec!["metadata_key", "Key"]), Some(vec![])] {
        let (mut source, mut manifest) = Objects::fixture();
        manifest["future"] = json!("x".repeat(1024 * 1024));
        source.manifest(&manifest);
        let mut reader = PmcInventoryAccessor::new(source)
            .open(request(fields))
            .await
            .unwrap();
        let schema = reader.metadata().schema.clone();
        assert!(schema.metadata()["pmc.inventory.raw_manifest_json"].len() > 1024 * 1024);
        let mut rows = 0;
        while let Some(batch) = reader.next().await.unwrap() {
            assert!(Arc::ptr_eq(&batch.schema(), &schema));
            rows += batch.num_rows();
        }
        assert_eq!(rows, 3);
    }
}

#[tokio::test]
async fn decoded_keys_preserve_literal_plus_and_decode_exactly_once() {
    let keys = [
        ("metadata/PMC1.1.json", "metadata/PMC1.1.json"),
        ("metadata%2FPMC2+3.json", "metadata/PMC2+3.json"),
        ("metadata%2FPMC%252F4.json", "metadata/PMC%2F4.json"),
        ("metadata%2F%E4%B8%AD.json", "metadata/中.json"),
    ];
    let (mut source, mut manifest) = Objects::fixture();
    manifest["files"].as_array_mut().unwrap().truncate(1);
    source.manifest(&manifest);
    let csv = keys
        .iter()
        .map(|(key, _)| format!("pmc-oa-opendata,{key},date,tag\n"))
        .collect::<String>();
    source.insert(FIRST, &gzip(csv.as_bytes()));
    let mut reader = PmcInventoryAccessor::new(source)
        .open(
            request(Some(vec!["metadata_key", "Key"])).with_options(ReadOptions {
                batch_size: NonZeroUsize::new(keys.len()).unwrap(),
                projection: Some(vec!["metadata_key".into(), "Key".into()]),
            }),
        )
        .await
        .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), keys.len());
    for (index, (encoded, decoded)) in keys.iter().enumerate() {
        for (column, expected) in [("metadata_key", decoded), ("Key", encoded)] {
            assert_eq!(
                batch
                    .column_by_name(column)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(index),
                *expected
            );
        }
    }
    assert!(reader.next().await.unwrap().is_none());
}
#[tokio::test]
async fn declared_field_order_extra_columns_and_csv_quoting_are_preserved() {
    let (mut s, mut m) = Objects::fixture();
    m["fileSchema"] = json!("ETag,Key,Bucket,LastModifiedDate,FutureField");
    m["files"].as_array_mut().unwrap().truncate(1);
    s.manifest(&m);
    s.insert(
        FIRST,
        &gzip(b"tag,metadata/PMC1.1.json,pmc-oa-opendata,date,\"quoted,extra\"\n"),
    );
    let mut r = PmcInventoryAccessor::new(s)
        .open(request(None))
        .await
        .unwrap();
    let b = r.next().await.unwrap().unwrap();
    assert_eq!(b.schema().field(0).name(), "ETag");
    assert_eq!(
        b.column_by_name("FutureField")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "quoted,extra"
    );
}
#[tokio::test]
async fn invalid_manifests_never_open_csv_shards() {
    for (field, value) in [
        ("sourceBucket", json!("other")),
        ("destinationBucket", json!("other")),
        ("version", json!("")),
        ("creationTimestamp", json!("yesterday")),
        ("fileFormat", json!("Parquet")),
        ("fileSchema", json!("Bucket,Key,Key,ETag")),
        (
            "fileSchema",
            json!("Bucket,Key,LastModifiedDate,ETag,metadata_key"),
        ),
        (
            "files",
            json!([{"key":"../escape.csv.gz","size":1,"MD5checksum":"0123456789abcdef0123456789abcdef"}]),
        ),
    ] {
        let (mut s, mut m) = Objects::fixture();
        m[field] = value;
        s.manifest(&m);
        assert_eq!(
            PmcInventoryAccessor::new(s.clone())
                .open(request(None))
                .await
                .err()
                .unwrap()
                .kind(),
            if field == "fileFormat" {
                ErrorKind::Unsupported
            } else {
                ErrorKind::Decode
            }
        );
        assert_eq!(s.opened.lock().unwrap().len(), 1);
    }
    let (mut s, _) = Objects::fixture();
    s.insert(MANIFEST, b"{broken");
    assert_eq!(
        PmcInventoryAccessor::new(s)
            .open(request(None))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Decode
    );
}
#[tokio::test]
async fn unsafe_or_missing_keys_fail_even_with_zero_column_projection() {
    for row in [
        "other,metadata/a.json,date,tag\n",
        "pmc-oa-opendata,,date,tag\n",
        "pmc-oa-opendata,metadata/%2e%2e/a.json,date,tag\n",
        "pmc-oa-opendata,%2Fmetadata/a.json,date,tag\n",
        "pmc-oa-opendata,metadata/%ZZ.json,date,tag\n",
        "pmc-oa-opendata,content/a.pdf,date,tag\n",
        "pmc-oa-opendata,metadata/a.json,date\n",
    ] {
        let (mut s, _) = Objects::fixture();
        s.insert(FIRST, &gzip(row.as_bytes()));
        let mut r = PmcInventoryAccessor::new(s)
            .open(request(Some(vec![])))
            .await
            .unwrap();
        assert_eq!(
            r.next().await.unwrap_err().kind(),
            ErrorKind::Decode,
            "{row}"
        );
        assert!(r.next().await.unwrap().is_none());
    }
}
#[tokio::test]
async fn empty_manifests_and_shards_are_eof_with_schema() {
    let (mut s, mut m) = Objects::fixture();
    m["files"] = json!([]);
    s.manifest(&m);
    let mut r = PmcInventoryAccessor::new(s.clone())
        .open(request(None))
        .await
        .unwrap();
    assert_eq!(r.metadata().schema.fields().len(), 5);
    assert!(r.next().await.unwrap().is_none());
    assert_eq!(s.opened.lock().unwrap().len(), 1);
    let (mut s, _) = Objects::fixture();
    s.insert(FIRST, &gzip(b""));
    let mut r = PmcInventoryAccessor::new(s)
        .open(request(None))
        .await
        .unwrap();
    assert_eq!(r.next().await.unwrap().unwrap().num_rows(), 1);
    assert!(r.next().await.unwrap().is_none());
}
#[tokio::test]
async fn dropping_stops_discovery_and_shard_errors_are_terminal() {
    let (s, _) = Objects::fixture();
    let mut r = PmcInventoryAccessor::new(s.clone())
        .open(request(None))
        .await
        .unwrap();
    r.next().await.unwrap().unwrap();
    drop(r);
    assert_eq!(s.opened.lock().unwrap().len(), 2);
    let (mut s, _) = Objects::fixture();
    s.files.remove(SECOND);
    let mut r = PmcInventoryAccessor::new(s)
        .open(request(None))
        .await
        .unwrap();
    for _ in 0..2 {
        r.next().await.unwrap().unwrap();
    }
    let e = r.next().await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::NotFound);
    assert!(std::error::Error::source(&e).is_some());
    assert!(r.next().await.unwrap().is_none());
    let (mut s, _) = Objects::fixture();
    s.insert(FIRST, b"bad gzip");
    let mut r = PmcInventoryAccessor::new(s)
        .open(request(None))
        .await
        .unwrap();
    assert_eq!(r.next().await.unwrap_err().kind(), ErrorKind::Decode);
    assert!(r.next().await.unwrap().is_none());
}
#[tokio::test]
async fn manifest_bounds_cover_known_and_decoded_lengths() {
    let (s, m) = Objects::fixture();
    assert_eq!(
        PmcInventoryAccessor::new(s)
            .with_manifest_limit(NonZeroUsize::MIN)
            .open(request(None))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Decode
    );
    let compressed = MemoryAccessor::new(&gzip(&serde_json::to_vec(&m).unwrap()), 1)
        .layer(DecompressionLayer::new(Gzip));
    assert_eq!(
        PmcInventoryAccessor::new(compressed)
            .with_manifest_limit(NonZeroUsize::new(10).unwrap())
            .open(request(None))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Decode
    );
    let (s, _) = Objects::fixture();
    let a = PmcInventoryAccessor::new(s.clone());
    for key in [
        "../manifest.json",
        "/snapshot/manifest.json",
        "snapshot/../manifest.json",
        "metadata/",
    ] {
        assert_eq!(
            a.open(ReadRequest::new(key)).await.err().unwrap().kind(),
            ErrorKind::InvalidInput
        );
    }
    assert!(s.opened.lock().unwrap().is_empty());
}
