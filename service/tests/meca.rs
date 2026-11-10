use arrow_array::cast::AsArray;
use futures::TryStreamExt;
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteReadRequest, ErrorKind, ReadOptions, ScanPlanner,
};
use open_arrow_access_decompression::zip::ZipLimits;
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_service::meca::{MecaArchive, MecaReadRequest};
use open_arrow_access_test_support::{MemoryAccessor, collect_bytes, fixture};
use std::num::NonZeroUsize;

async fn open(name: &str) -> open_arrow_access_core::Result<MecaArchive> {
    let data = fixture!("meca", name).await;
    let input = MemoryAccessor::new(&data, 7)
        .with_ranges()
        .open(ByteReadRequest::new("bundle.meca"))
        .await?;
    MecaArchive::open(input, ZipLimits::default()).await
}

#[tokio::test]
async fn original_manifest_and_independent_members_are_retained() {
    let archive = open("valid.meca").await.unwrap();
    assert!(archive.manifest().starts_with(b"<?xml"));
    assert_eq!(archive.files().len(), 2);
    assert_eq!(archive.files()[1].href, "content/paper%20one.pdf");
    assert_eq!(archive.archive().entries().len(), 4);
    let mut pdf = archive
        .archive()
        .open_entry(
            archive.files()[1].entry_index,
            NonZeroUsize::new(2).unwrap(),
        )
        .await
        .unwrap();
    drop(archive);
    assert_eq!(collect_bytes(pdf.as_mut()).await.unwrap(), b"%PDF-test");
}

#[tokio::test]
async fn missing_ambiguous_and_unsafe_references_fail() {
    for name in [
        "missing.meca",
        "unsafe.meca",
        "duplicate.meca",
        "wrong-root.meca",
    ] {
        assert_eq!(
            open(name).await.err().unwrap().kind(),
            ErrorKind::Decode,
            "{name}"
        );
    }
}

#[tokio::test]
async fn native_parallel_entries_preserve_manifest_fields_and_binary_data() {
    for projection in [None, Some(vec!["data".into(), "href".into()]), Some(vec![])] {
        let archive = open("valid.meca").await.unwrap();
        let files = archive.files().to_vec();
        let records = archive.records();
        let request = MecaReadRequest::default().with_options(ReadOptions {
            projection,
            batch_size: NonZeroUsize::MIN,
        });
        let plan = records.plan_scan(request.clone()).await.unwrap();
        assert_eq!(plan.partitions.size_hint().0, files.len());
        let mut expected = records.open(request.clone()).await.unwrap();
        let mut actual = records
            .layer(ParallelReadLayer::new(
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::MIN,
            ))
            .open(request)
            .await
            .unwrap();
        let mut index = 0;
        while let Some(batch) = expected.next().await.unwrap() {
            assert_eq!(actual.next().await.unwrap().unwrap(), batch);
            if let Some(href) = batch.column_by_name("href") {
                assert_eq!(href.as_string::<i64>().value(0), files[index].href);
                if files[index].media_type.as_deref() == Some("application/pdf") {
                    assert_eq!(batch["data"].as_binary::<i64>().value(0), b"%PDF-test");
                }
            }
            index += batch.num_rows();
        }
        assert_eq!(index, files.len());
        assert!(actual.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn planning_and_opening_partitions_do_not_read_entry_bodies() {
    let data = fixture!("meca", "valid.meca").await;
    let source = MemoryAccessor::new(&data, 7).with_ranges();
    let observed = source.observations.clone();
    let archive = MecaArchive::open(
        source
            .open(ByteReadRequest::new("bundle.meca"))
            .await
            .unwrap(),
        ZipLimits::default(),
    )
    .await
    .unwrap();
    let reads = observed
        .range_reads
        .load(std::sync::atomic::Ordering::SeqCst);
    let records = archive.records();
    let mut plan = records.plan_scan(MecaReadRequest::default()).await.unwrap();
    let partition = plan.partitions.try_next().await.unwrap().unwrap();
    let mut reader = partition.await.unwrap();
    assert_eq!(
        observed
            .range_reads
            .load(std::sync::atomic::Ordering::SeqCst),
        reads
    );
    reader.next().await.unwrap().unwrap();
    assert!(
        observed
            .range_reads
            .load(std::sync::atomic::Ordering::SeqCst)
            > reads
    );
}
