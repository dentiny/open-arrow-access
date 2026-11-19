use arrow_array::{StringArray, cast::AsArray};
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteAccessor, ByteReadOptions, ByteReadRequest, ByteReader, Capability,
    Error, ErrorKind, ReadOptions, ReadRequest, Result, ScanPlanner, async_trait,
};
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_service::pmc_inventory::PmcInventoryAccessor;
use open_arrow_access_test_support::{
    MemoryAccessor, fixture,
    record::{assert_same_batches, column, field_names},
};
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
        field_names(&reader.metadata().schema),
        ["ETag", "Key", "Bucket", "LastModifiedDate", "FutureField"]
    );
    let mut keys = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        assert!(Arc::ptr_eq(&batch.schema(), &reader.metadata().schema));
        assert_eq!(batch["FutureField"].as_string::<i32>().value(0), "extra");
        let column = column::<StringArray>(&batch, "Key");
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
            assert_eq!(field_names(batch.schema_ref()), names);
            rows += batch.num_rows();
        }
        assert_eq!(rows, 3);
    }
}

#[tokio::test]
async fn invalid_manifests_fail_before_opening_shards() {
    for name in ["bad_bucket.json", "bad_timestamp.json", "bad_schema.json"] {
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

#[tokio::test]
async fn parallel_inventory_matches_order_metadata_projection_and_validation() {
    for projection in [None, Some(vec!["ETag", "Key"]), Some(vec![])] {
        let source = Objects::fixture().await;
        let accessor = PmcInventoryAccessor::new(source.clone());
        let plan = accessor
            .plan_scan(request(projection.clone()))
            .await
            .unwrap();
        assert_eq!(plan.partitions.size_hint().0, 2);
        assert_eq!(source.opened.lock().unwrap().len(), 1);
        drop(plan);
        let mut expected = accessor.open(request(projection.clone())).await.unwrap();
        let mut actual = accessor
            .layer(ParallelReadLayer::new(
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::new(1).unwrap(),
            ))
            .open(request(projection))
            .await
            .unwrap();
        assert_same_batches(expected.as_mut(), actual.as_mut()).await;
    }
    let mut source = Objects::fixture().await;
    source.insert(
        SECOND,
        &fixture!("pmc/inventory", "bad_bucket.csv.gz").await,
    );
    let mut reader = PmcInventoryAccessor::new(source)
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ))
        .open(request(Some(vec![])))
        .await
        .unwrap();
    loop {
        match reader.next().await {
            Err(_) => break,
            Ok(Some(_)) => {}
            Ok(None) => panic!("invalid bucket bypassed validation"),
        }
    }
}

#[tokio::test]
async fn forwards_shard_keys_without_path_validation() {
    let mut source = Objects::fixture().await;
    source.insert(
        MANIFEST,
        &fixture!("pmc/inventory", "bad_shard_path.json").await,
    );
    source.insert(
        "../escape.csv.gz",
        &fixture!("pmc/inventory", "first.csv.gz").await,
    );
    let opened = source.opened.clone();
    let mut reader = PmcInventoryAccessor::new(source)
        .open(request(None))
        .await
        .unwrap();
    assert!(reader.next().await.unwrap().is_some());
    assert_eq!(
        opened.lock().unwrap()[1].object.locator(),
        "../escape.csv.gz"
    );
}

#[tokio::test]
async fn real_inventory_shard_reads_only_its_referenced_article_metadata() {
    use open_arrow_access_service::pmc::{PmcInventoryShardAccessor, PmcInventoryShardRequest};
    const SNAPSHOT: &str =
        "inventory-reports/pmc-oa-opendata/metadata/2026-11-18T01-00Z/manifest.json";
    const SHARD: &str = "inventory-reports/pmc-oa-opendata/metadata/data/13cbabe4-91e8-4d22-977f-adc2fc7eb6cb.csv.gz";
    let mut source = Objects::default();
    source.insert(
        SNAPSHOT,
        &fixture!("pmc/inventory/upstream", "manifest.json").await,
    );
    source.insert(
        SHARD,
        &fixture!("pmc/inventory/upstream", "shard.csv.gz").await,
    );
    let ids = ["PMC5000000", "PMC5000001", "PMC5000002"];
    for id in ids {
        let filename = format!("{id}.1.json");
        source.insert(
            &format!("metadata/{filename}"),
            &fixture!("pmc/inventory/upstream", &filename).await,
        );
    }
    let shards = PmcInventoryAccessor::new(source.clone())
        .shards(ByteReadRequest::new(SNAPSHOT))
        .await
        .unwrap();
    assert_eq!(shards.len(), 11);
    assert_eq!(shards[0].locator(), SHARD);
    assert_eq!(source.opened.lock().unwrap().len(), 1);
    for projection in [
        None,
        Some(vec!["pmcid".into(), "pmid".into(), "pdf_url".into()]),
        Some(vec![]),
    ] {
        source.opened.lock().unwrap().clear();
        let options = ByteReadOptions {
            io_concurrency: NonZeroUsize::new(3).unwrap(),
            io_chunk_size: NonZeroUsize::new(7),
        };
        let mut reader = PmcInventoryShardAccessor::new(source.clone())
            .layer(ParallelReadLayer::new(
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::MIN,
            ))
            .open(
                PmcInventoryShardRequest::new(SNAPSHOT, SHARD)
                    .with_options(ReadOptions {
                        batch_size: NonZeroUsize::MIN,
                        projection: projection.clone(),
                    })
                    .with_byte_options(options),
            )
            .await
            .unwrap();
        // Opening the plan reads just the manifest; CSV and JSON reads are lazy.
        assert_eq!(source.opened.lock().unwrap().len(), 1);
        let mut rows = 0;
        while let Some(batch) = reader.next().await.unwrap() {
            if projection
                .as_ref()
                .is_none_or(|columns| !columns.is_empty())
            {
                assert_eq!(batch["pmcid"].as_string::<i32>().value(0), ids[rows]);
                assert_eq!(batch["pmid"].data_type(), &arrow_schema::DataType::UInt64);
                assert!(batch.schema().field_with_name("metadata").is_err());
            }
            rows += batch.num_rows();
        }
        assert_eq!(rows, 3);
        let opened = source.opened.lock().unwrap();
        assert_eq!(opened.len(), 5); // One manifest, one CSV, three article JSONs; no PDFs.
        assert_eq!(opened[1].object.locator(), SHARD);
        assert!(opened.iter().all(|request| request.options == options));
    }
}

#[tokio::test]
async fn selected_inventory_paths_decode_keys_and_keep_duplicate_records() {
    use futures::TryStreamExt;
    let paths = PmcInventoryAccessor::new(Objects::fixture().await)
        .with_shard(FIRST)
        .metadata_paths(request(None))
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        paths.iter().map(|path| path.locator()).collect::<Vec<_>>(),
        ["metadata/PMC1.1.json", "metadata/PMC1.1.json"]
    );
}

#[tokio::test]
async fn article_shards_reject_unknown_shards_and_validate_inventory_rows() {
    use open_arrow_access_service::pmc::{PmcInventoryShardAccessor, PmcInventoryShardRequest};
    let mut source = Objects::fixture().await;
    let error = PmcInventoryShardAccessor::new(source.clone())
        .open(PmcInventoryShardRequest::new(
            MANIFEST,
            "data/missing.csv.gz",
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(source.opened.lock().unwrap().len(), 1);
    source.insert(FIRST, &fixture!("pmc/inventory", "bad_bucket.csv.gz").await);
    let mut reader = PmcInventoryShardAccessor::new(source)
        .open(
            PmcInventoryShardRequest::new(MANIFEST, FIRST)
                .with_options(ReadOptions::default().with_projection(Vec::<String>::new())),
        )
        .await
        .unwrap();
    assert!(reader.next().await.is_err());
}
