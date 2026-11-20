use super::*;
use arrow_array::{ArrayRef, LargeBinaryArray, LargeStringArray, StringArray, UInt64Array};
use futures::TryStreamExt;
use open_arrow_access_core::AccessorExt;
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::fs_operator;
use std::num::NonZeroUsize;

fn input(name: &[u8], data: &[u8]) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "shard_uri",
            Arc::new(LargeStringArray::from(vec!["src/arXiv_src_2410_001.tar"])) as ArrayRef,
        ),
        (
            "entry_index",
            Arc::new(UInt64Array::from(vec![7])) as ArrayRef,
        ),
        (
            "entry_name",
            Arc::new(LargeBinaryArray::from(vec![name])) as ArrayRef,
        ),
        (
            "data",
            Arc::new(LargeBinaryArray::from(vec![data])) as ArrayRef,
        ),
    ])
    .unwrap()
}
#[tokio::test]
async fn expands_tar_and_gzip_without_assets_and_preserves_duplicate_identity() {
    for data in [
        include_bytes!("../../../../testdata/arxiv-articles/source.tar").as_slice(),
        include_bytes!("../../../../testdata/arxiv-articles/source.tar.gz").as_slice(),
    ] {
        let batches = expand(
            &input(b"2410/2410.00001v2.gz", data),
            &ReadOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        let batch = &batches[0];
        assert_eq!(
            batch["text"].as_string::<i64>().value(0).as_bytes(),
            include_bytes!("../../../../testdata/arxiv-articles/members/main.tex")
        );
        assert_eq!(
            batch["source_name"].as_binary::<i64>().value(0),
            b"main.tex"
        );
        assert_eq!(
            batch["article_id"].as_string::<i64>().value(0),
            "2410.00001"
        );
        assert_eq!(
            batch["entry_name"].as_binary::<i64>().value(0),
            b"2410/2410.00001v2.gz"
        );
        assert_eq!(
            batch["entry_index"]
                .as_primitive::<arrow_array::types::UInt64Type>()
                .value(0),
            7
        );
        assert_ne!(
            batch["source_index"]
                .as_primitive::<arrow_array::types::UInt64Type>()
                .value(0),
            batch["source_index"]
                .as_primitive::<arrow_array::types::UInt64Type>()
                .value(1)
        );
        let snapshot = RecordBatch::try_from_iter(vec![(
            "identifier",
            Arc::new(StringArray::from(vec!["oai:arXiv.org:2410.00001"])) as ArrayRef,
        )])
        .unwrap();
        let index = ArxivMetadataIndex::try_new(snapshot, "identifier").unwrap();
        assert_eq!(index.attach_articles(batch.clone()).unwrap().num_rows(), 2);
    }
}
#[tokio::test]
async fn supports_single_gzip_and_legacy_sources_and_rejects_non_latex() {
    for data in [
        include_bytes!("../../../../testdata/arxiv-articles/main.tex").as_slice(),
        include_bytes!("../../../../testdata/arxiv-articles/main.tex.gz").as_slice(),
    ] {
        let output = expand(&input(b"hep-th/9901001.gz", data), &ReadOptions::default())
            .await
            .unwrap();
        assert_eq!(
            output[0]["article_id"].as_string::<i64>().value(0),
            "hep-th9901001"
        );
    }
    for data in [
        b"%PDF-1.7".as_slice(),
        b"%!PS",
        b"plain text",
        b"\xff",
        include_bytes!("../../../../testdata/arxiv-articles/bad-crc.tar.gz"),
    ] {
        assert!(
            expand(&input(b"2410/2410.00001.gz", data), &ReadOptions::default())
                .await
                .is_err()
        );
    }
    assert!(!is_tex(b"anc/extra.tex").unwrap());
    assert!(!is_tex(b"nested/ANC/extra.tex").unwrap());
    assert!(is_tex(b"chapters/intro.tex").unwrap());
    assert!(is_tex(b"../escape.tex").is_err());
}

fn tar(name: &str, payload: &[u8]) -> Vec<u8> {
    let mut header = [0_u8; 512];
    header[..name.len()].copy_from_slice(name.as_bytes());
    header[100..108].copy_from_slice(b"0000644\0");
    header[124..136].copy_from_slice(format!("{:011o}\0", payload.len()).as_bytes());
    header[148..156].fill(b' ');
    header[156] = b'0';
    let checksum: u64 = header.iter().map(|&b| u64::from(b)).sum();
    header[148..156].copy_from_slice(format!("{checksum:06o}\0 ").as_bytes());
    let mut output = header.to_vec();
    output.extend_from_slice(payload);
    output.resize(output.len().div_ceil(512) * 512 + 1024, 0);
    output
}
#[tokio::test]
async fn preserves_shard_partitions_and_joins_source_metadata_before_projection() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::create_dir(temp.path().join("src")).unwrap();
    let payload = include_bytes!("../../../../testdata/arxiv-articles/source.tar.gz");
    let mut manifest = String::from("<arXivSRC>");
    for sequence in 1..=2 {
        let name = format!("src/arXiv_src_2410_{sequence:03}.tar");
        std::fs::write(
            temp.path().join(&name),
            tar("2410/2410.00001v2.gz", payload),
        )
        .unwrap();
        manifest.push_str(&format!("<file><filename>{name}</filename><md5sum>00000000000000000000000000000000</md5sum><num_items>1</num_items><seq_num>{sequence}</seq_num><size>{}</size><timestamp>2024-10-31 00:00:00</timestamp><yymm>2410</yymm></file>",payload.len()));
    }
    manifest.push_str("</arXivSRC>");
    std::fs::write(temp.path().join("src/arXiv_src_manifest.xml"), manifest).unwrap();
    let records = crate::arxiv::ArxivService::new(OpenDalAccessor::new(fs_operator(temp.path())))
        .records()
        .latex();
    let request = ArxivReadRequest::new(ArxivKind::Source);
    let plan = records.plan_scan(request.clone()).await.unwrap();
    assert_eq!(
        plan.partitions.try_collect::<Vec<_>>().await.unwrap().len(),
        2
    );
    let snapshot = RecordBatch::try_from_iter(vec![(
        "identifier",
        Arc::new(arrow_array::StringArray::from(vec!["2410.00001"])) as arrow_array::ArrayRef,
    )])
    .unwrap();
    let records = records.with_metadata(Arc::new(
        ArxivMetadataIndex::try_new(snapshot, "identifier").unwrap(),
    ));
    let mut request = request;
    request.options.projection = Some(vec!["metadata".into(), "text".into()]);
    let mut reader = records
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::MIN,
        ))
        .open(request)
        .await
        .unwrap();
    let mut count = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.num_columns(), 2);
        assert!(batch.column_by_name("metadata").is_some());
        count += batch.num_rows();
    }
    assert_eq!(count, 4);
}
