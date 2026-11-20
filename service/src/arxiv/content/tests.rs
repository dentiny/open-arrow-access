use super::*;
use crate::arxiv::ArxivKind;
use arrow_array::{Array, StructArray, types::UInt64Type};
use open_arrow_access_test_support::fixture;

fn raw_batch(name: &str, data: &[u8]) -> Result<RecordBatch> {
    let mut input = RecordBatchBuilder::new(crate::arxiv::record_schema(), None)?;
    input.push(|row| {
        row.append("shard_uri", "src/arXiv_src_2410_001.tar")?;
        row.append("entry_index", 7_u64)?;
        row.append("entry_name", name.as_bytes())?;
        row.append("data", data)
    })?;
    input.finish()
}

async fn read(name: &str, data: &[u8]) -> Result<Vec<RecordBatch>> {
    expand(
        &raw_batch(name, data)?,
        &ReadOptions::default().with_batch_size(std::num::NonZeroUsize::MIN),
    )
    .await
}

#[tokio::test]
async fn packages_keep_only_tex_with_provenance() {
    let original = fixture!("arxiv-articles", "members/main.tex").await;
    for file in ["source.tar", "source.tar.gz"] {
        let data = fixture!("arxiv-articles", file).await;
        let batches = read("2410/2410.00001v2.gz", &data).await.unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert!(batches.iter().all(|batch| batch.num_rows() == 1));
        let batch = arrow_select::concat::concat_batches(&content_schema(), &batches).unwrap();
        assert_eq!(batch["data"].as_binary::<i64>().value(0), original);
        assert!(
            batch["source_name"]
                .as_binary::<i64>()
                .iter()
                .all(|name| name == Some(b"main.tex".as_slice()))
        );
        assert_eq!(
            batch["entry_name"].as_binary::<i64>().value(0),
            b"2410/2410.00001v2.gz"
        );
        let indices = batch["source_index"].as_primitive::<UInt64Type>();
        assert_ne!(indices.value(0), indices.value(1));
    }
}

#[tokio::test]
async fn accepts_legacy_single_sources_and_rejects_non_latex() {
    for file in ["main.tex", "main.tex.gz"] {
        let batches = read("hep-th/9901001.gz", &fixture!("arxiv-articles", file).await)
            .await
            .unwrap();
        assert_eq!(
            batches[0]["article_id"].as_string::<i64>().value(0),
            "hep-th9901001"
        );
    }
    for data in [b"%PDF-1.7".as_slice(), b"%!PS", b"plain text", b"\xff"] {
        assert!(read("2410/2410.00001.gz", data).await.is_err());
    }
    let corrupt = fixture!("arxiv-articles", "bad-crc.tar.gz").await;
    assert!(read("2410/2410.00001.gz", &corrupt).await.is_err());
    for name in [
        b"anc/extra.tex".as_slice(),
        b"nested/ANC/extra.tex",
        b"figure.png",
    ] {
        assert!(!validation::is_tex(name).unwrap());
    }
    assert!(validation::is_tex(b"chapters/intro.tex").unwrap());
    assert!(validation::is_tex(b"../escape.tex").is_err());
}

#[tokio::test]
async fn formats_preserve_content_and_join_metadata() {
    use open_arrow_access_storage::OpenDalAccessor;
    use open_arrow_access_test_support::temp_fs;
    let schema = content_schema();
    let snapshot = oai_snapshot().await;
    assert_eq!(schema.fields().len(), 10);
    let metadata = schema.field_with_name("metadata").unwrap();
    assert!(metadata.is_nullable());
    let data_type = schema.field_with_name("data_type").unwrap();
    assert_eq!(data_type.data_type(), &DataType::LargeUtf8);
    assert!(!data_type.is_nullable());
    for (format, kind, fixture_name) in [
        (ArxivContentFormat::Pdf, ArxivKind::Pdf, "pdf.tar"),
        (ArxivContentFormat::Latex, ArxivKind::Source, "latex.tar"),
        (ArxivContentFormat::Xml, ArxivKind::Source, "xml.tar"),
    ] {
        let bytes = fixture!("arxiv-content", fixture_name).await;
        let (prefix, root) = if kind == ArxivKind::Pdf {
            ("pdf", "arXivPDF")
        } else {
            ("src", "arXivSRC")
        };
        let key = format!("{prefix}/arXiv_{prefix}_2410_001.tar");
        let manifest = format!(
            "<{root}><file><filename>{key}</filename><md5sum>00000000000000000000000000000000</md5sum><num_items>1</num_items><seq_num>1</seq_num><size>{}</size><timestamp>2024-10-31 00:00:00</timestamp><yymm>2410</yymm></file></{root}>",
            bytes.len()
        );
        let manifest_key = format!("{prefix}/arXiv_{prefix}_manifest.xml");
        let (_temp, operator) = temp_fs(&[(&manifest_key, manifest.as_bytes()), (&key, &bytes)]);
        let records = crate::arxiv::ArxivService::new(OpenDalAccessor::new(operator))
            .records()
            .content(format);
        let mut reader = records.open(ArxivReadRequest::new(kind)).await.unwrap();
        assert_eq!(reader.metadata().schema, schema);
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.schema(), schema);
        assert_eq!(batch["metadata"].null_count(), batch.num_rows());
        assert_eq!(
            batch["article_id"].as_string::<i64>().value(0),
            "2410.00001"
        );
        assert_eq!(batch["format"].as_string::<i64>().value(0), format.as_str());
        assert_eq!(
            batch["data_type"].as_string::<i64>().value(0),
            format.as_str()
        );
        let expected = match format {
            ArxivContentFormat::Pdf => fixture!("arxiv-articles", "paper.pdf").await,
            ArxivContentFormat::Latex => fixture!("arxiv-articles", "main.tex").await,
            ArxivContentFormat::Xml => fixture!("arxiv-xml", "article.xml").await,
        };
        assert_eq!(batch["data"].as_binary::<i64>().value(0), expected);
        assert!(reader.next().await.unwrap().is_none());
        let records = records.with_metadata(index(snapshot.clone())).unwrap();
        let request = ArxivReadRequest::new(kind).with_month(2410);
        let mut reader = records.open(request.clone()).await.unwrap();
        let enriched = reader.next().await.unwrap().unwrap();
        assert_eq!(enriched["data"].as_ref(), batch["data"].as_ref());
        assert_metadata(&enriched, &snapshot, 1);
        assert!(reader.next().await.unwrap().is_none());
        // Reordered and empty projections still preserve row counts.
        for columns in [vec!["metadata".into(), "article_id".into()], vec![]] {
            let mut request = request.clone();
            request.options.projection = Some(columns.clone());
            let mut reader = records.open(request).await.unwrap();
            let batch = reader.next().await.unwrap().unwrap();
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), columns.len());
            if !columns.is_empty() {
                assert_metadata(&batch, &snapshot, 1);
            }
            assert!(reader.next().await.unwrap().is_none());
        }
        // Missing matches fail even when metadata is projected away.
        let records = records.with_metadata(index(snapshot.slice(2, 1))).unwrap();
        let mut request = ArxivReadRequest::new(kind).with_shard(key);
        request.options.projection = Some(vec![]);
        let mut reader = records.open(request).await.unwrap();
        assert!(reader.next().await.is_err());
    }
}

async fn oai_snapshot() -> RecordBatch {
    use crate::{
        arxiv_metadata::ArxivMetadata,
        oai::{OaiPage, OaiProvider},
    };
    use open_arrow_access_core::ReadRequest;
    use open_arrow_access_test_support::format::collect_batches;
    let page = OaiPage {
        bytes: fixture!("arxiv-content", "metadata.xml").await.into(),
        record_count: 3,
        resumption_token: None,
    };
    collect_batches(
        &page.records(ArxivMetadata::inferred_record_format()),
        ReadRequest::new("metadata"),
    )
    .await
    .remove(0)
}

fn index(snapshot: RecordBatch) -> Arc<ArxivMetadataIndex> {
    Arc::new(ArxivMetadataIndex::from_oai_records(snapshot).unwrap())
}

fn assert_metadata(batch: &RecordBatch, snapshot: &RecordBatch, expected_row: usize) {
    let expected = StructArray::from(snapshot.clone()).slice(expected_row, 1);
    let metadata = batch["metadata"].as_struct();
    assert_eq!(metadata.null_count(), 0);
    for row in 0..batch.num_rows() {
        assert_eq!(metadata.slice(row, 1), expected);
    }
}

#[tokio::test]
async fn metadata_normalizes_legacy_ids_repeats_tex_rows_and_retains_deletions() {
    let snapshot = oai_snapshot().await;
    let index = ArxivMetadataIndex::from_oai_records(snapshot.clone()).unwrap();
    let batches = read(
        "hep-th/9901001v2.gz",
        &fixture!("arxiv-articles", "source.tar.gz").await,
    )
    .await
    .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    for batch in batches {
        assert_metadata(&index.attach_content(batch).unwrap(), &snapshot, 0);
    }
    let deleted = document_batch(
        &raw_batch("2410/2410.99999.pdf", b"%PDF").unwrap(),
        ArxivContentFormat::Pdf,
    )
    .unwrap();
    assert_metadata(&index.attach_content(deleted).unwrap(), &snapshot, 2);
    assert!(snapshot["oai:metadata"].is_null(2));
    let duplicate = arrow_select::concat::concat_batches(
        &snapshot.schema(),
        &[snapshot.slice(1, 1), snapshot.slice(1, 1)],
    )
    .unwrap();
    assert!(ArxivMetadataIndex::from_oai_records(duplicate).is_err());
    let empty = RecordBatch::new_empty(crate::arxiv::record_schema());
    assert!(ArxivMetadataIndex::from_oai_records(empty).is_err());
}

#[tokio::test]
async fn rejects_null_ids_and_untyped_content_snapshots() {
    use arrow_array::{ArrayRef, StringArray};
    use open_arrow_access_storage::OpenDalAccessor;
    use open_arrow_access_test_support::temp_fs;
    let snapshot = oai_snapshot().await;
    let header = snapshot["oai:header"].as_struct();
    let mut header_columns = header.columns().to_vec();
    header_columns[0] = Arc::new(StringArray::from(vec![None::<&str>; snapshot.num_rows()]));
    let header = StructArray::new(header.fields().clone(), header_columns, None);
    let mut columns = snapshot.columns().to_vec();
    columns[0] = Arc::new(header);
    let invalid = RecordBatch::try_new(snapshot.schema(), columns).unwrap();
    assert!(ArxivMetadataIndex::from_oai_records(invalid).is_err());
    // Older raw-PDF indexes remain usable there, but cannot change the fixed content schema.
    let flat = RecordBatch::try_from_iter(vec![(
        "identifier",
        Arc::new(StringArray::from(vec!["2410.00001"])) as ArrayRef,
    )])
    .unwrap();
    let index = Arc::new(ArxivMetadataIndex::try_new(flat, "identifier").unwrap());
    let (_temp, operator) = temp_fs(&[]);
    assert!(
        crate::arxiv::ArxivService::new(OpenDalAccessor::new(operator))
            .records()
            .content(ArxivContentFormat::Pdf)
            .with_metadata(index)
            .is_err()
    );
    let empty = RecordBatch::new_empty(snapshot.schema());
    assert!(ArxivMetadataIndex::from_oai_records(empty).is_ok());
}
