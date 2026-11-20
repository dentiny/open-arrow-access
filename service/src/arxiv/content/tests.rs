use super::*;
use crate::arxiv::ArxivKind;
use arrow_array::types::UInt64Type;
use open_arrow_access_test_support::fixture;

async fn read(name: &str, data: &[u8]) -> Result<Vec<RecordBatch>> {
    let mut input = RecordBatchBuilder::new(crate::arxiv::record_schema(), None)?;
    input.push(|row| {
        row.append("shard_uri", "src/arXiv_src_2410_001.tar")?;
        row.append("entry_index", 7_u64)?;
        row.append("entry_name", name.as_bytes())?;
        row.append("data", data)
    })?;
    expand(
        &input.finish()?,
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
async fn formats_emit_identical_schemas_and_preserve_content() {
    use open_arrow_access_storage::OpenDalAccessor;
    use open_arrow_access_test_support::temp_fs;
    let schema = content_schema();
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
        let index = Arc::new(
            ArxivMetadataIndex::from_oai_records(oai_snapshot(&["2410.00001"]).await).unwrap(),
        );
        let records = records.with_metadata(index).unwrap();
        let mut reader = records
            .open(ArxivReadRequest::new(kind).with_month(2410))
            .await
            .unwrap();
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch["metadata"].null_count(), 0);
        assert!(reader.next().await.unwrap().is_none());
    }
}

async fn oai_snapshot(ids: &[&str]) -> RecordBatch {
    use crate::{arxiv_metadata::ArxivMetadataService, oai::OaiPage};
    use open_arrow_access_core::ReadRequest;
    use open_arrow_access_test_support::format::collect_batches;
    let mut xml = String::from(
        r#"<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:a="http://arxiv.org/OAI/arXivRaw/"><ListRecords>"#,
    );
    for id in ids {
        xml.push_str(&format!(r#"<record><header><identifier>oai:arXiv.org:{id}</identifier><datestamp>2026-11-20</datestamp><setSpec>physics</setSpec></header><metadata><a:arXivRaw><a:id>{id}</a:id><a:title>Title for {id}</a:title><a:authors>Author</a:authors><a:abstract>Abstract</a:abstract><a:doi>10.1234/example</a:doi><a:license>CC-BY</a:license><a:version version="v1"><a:date>2024-10-01</a:date></a:version><a:version version="v2"><a:date>2024-10-02</a:date></a:version></a:arXivRaw></metadata></record>"#));
    }
    xml.push_str(r#"<record><header status="deleted"><identifier>oai:arXiv.org:2410.99999</identifier><datestamp>2026-11-20</datestamp></header></record></ListRecords></OAI-PMH>"#);
    let page = OaiPage {
        bytes: Bytes::from(xml),
        record_count: ids.len() + 1,
        resumption_token: None,
    };
    collect_batches(
        &page.records(
            ArxivMetadataService::new()
                .unwrap()
                .inferred_record_format(),
        ),
        ReadRequest::new("metadata"),
    )
    .await
    .remove(0)
}

#[tokio::test]
async fn content_fills_full_metadata_for_all_formats_and_projections() {
    use arrow_array::{Array, StructArray};
    use open_arrow_access_storage::OpenDalAccessor;
    use open_arrow_access_test_support::temp_fs;
    // Deliberately reverse snapshot order relative to content, including a legacy ID.
    let snapshot = oai_snapshot(&["hep-th/9901001", "2410.00001"]).await;
    let index = Arc::new(ArxivMetadataIndex::from_oai_records(snapshot.clone()).unwrap());
    let expected = StructArray::from(snapshot).slice(1, 1);
    for (format, kind, file, key) in [
        (
            ArxivContentFormat::Pdf,
            ArxivKind::Pdf,
            "pdf.tar",
            "pdf/test.tar",
        ),
        (
            ArxivContentFormat::Latex,
            ArxivKind::Source,
            "latex.tar",
            "src/test.tar",
        ),
        (
            ArxivContentFormat::Xml,
            ArxivKind::Source,
            "xml.tar",
            "src/test.tar",
        ),
    ] {
        let bytes = fixture!("arxiv-content", file).await;
        let (_temp, operator) = temp_fs(&[(key, &bytes)]);
        let records = crate::arxiv::ArxivService::new(OpenDalAccessor::new(operator))
            .records()
            .content(format)
            .with_metadata(index.clone())
            .unwrap();
        for projection in [
            None,
            Some(vec!["metadata".to_owned(), "article_id".to_owned()]),
            Some(vec![]),
        ] {
            let mut request = ArxivReadRequest::new(kind).with_shard(key);
            request.options.projection = projection.clone();
            let mut reader = records.open(request).await.unwrap();
            let mut rows = 0;
            while let Some(batch) = reader.next().await.unwrap() {
                rows += batch.num_rows();
                if projection.as_ref().is_none_or(|p| !p.is_empty()) {
                    let metadata = batch["metadata"].as_struct();
                    assert_eq!(metadata.null_count(), 0);
                    for row in 0..batch.num_rows() {
                        assert_eq!(metadata.slice(row, 1), expected);
                    }
                    if projection.is_none() {
                        let expected_bytes = match format {
                            ArxivContentFormat::Pdf => {
                                fixture!("arxiv-articles", "paper.pdf").await
                            }
                            ArxivContentFormat::Latex => {
                                fixture!("arxiv-articles", "main.tex").await
                            }
                            ArxivContentFormat::Xml => fixture!("arxiv-xml", "article.xml").await,
                        };
                        assert_eq!(batch["data"].as_binary::<i64>().value(0), expected_bytes);
                    }
                }
            }
            assert_eq!(rows, 1);
        }
        // A missing match must also fail if the caller omits metadata entirely.
        let missing =
            Arc::new(ArxivMetadataIndex::from_oai_records(oai_snapshot(&[]).await).unwrap());
        let bytes = fixture!("arxiv-content", file).await;
        let (_temp, operator) = temp_fs(&[(key, &bytes)]);
        let records = crate::arxiv::ArxivService::new(OpenDalAccessor::new(operator))
            .records()
            .content(format)
            .with_metadata(missing)
            .unwrap();
        let mut request = ArxivReadRequest::new(kind).with_shard(key);
        request.options.projection = Some(vec![]);
        let mut reader = records.open(request).await.unwrap();
        assert!(reader.next().await.is_err());
    }
}

#[tokio::test]
async fn metadata_normalizes_legacy_ids_repeats_tex_rows_and_retains_deletions() {
    use arrow_array::{Array, StructArray};
    let snapshot = oai_snapshot(&["hep-th/9901001"]).await;
    let index = ArxivMetadataIndex::from_oai_records(snapshot.clone()).unwrap();
    let batches = read(
        "hep-th/9901001v2.gz",
        &fixture!("arxiv-articles", "source.tar.gz").await,
    )
    .await
    .unwrap();
    let mut rows = 0;
    for batch in batches {
        let batch = index.attach_content(batch).unwrap();
        rows += batch.num_rows();
        for row in 0..batch.num_rows() {
            assert_eq!(
                batch["metadata"].as_struct().slice(row, 1),
                StructArray::from(snapshot.clone()).slice(0, 1)
            );
        }
    }
    assert_eq!(rows, 2);
    let mut input = RecordBatchBuilder::new(crate::arxiv::record_schema(), None).unwrap();
    input
        .push(|row| {
            row.append("shard_uri", "pdf/test.tar")?;
            row.append("entry_index", 0_u64)?;
            row.append("entry_name", b"2410/2410.99999.pdf".as_slice())?;
            row.append("data", b"%PDF".as_slice())
        })
        .unwrap();
    let batch = index
        .attach_content(document_batch(&input.finish().unwrap(), ArxivContentFormat::Pdf).unwrap())
        .unwrap();
    let metadata = batch["metadata"].as_struct();
    assert!(!metadata.is_null(0));
    assert!(metadata["oai:metadata"].is_null(0));
    assert_eq!(
        metadata["oai:header"].as_struct()["@status"]
            .as_string::<i32>()
            .value(0),
        "deleted"
    );
    assert!(
        ArxivMetadataIndex::from_oai_records(oai_snapshot(&["2410.00001", "2410.00001"]).await)
            .is_err()
    );
    let empty = RecordBatch::new_empty(crate::arxiv::record_schema());
    assert!(ArxivMetadataIndex::from_oai_records(empty).is_err());
}

#[tokio::test]
async fn rejects_null_ids_and_untyped_content_snapshots() {
    use arrow_array::{ArrayRef, StringArray, StructArray};
    use open_arrow_access_storage::OpenDalAccessor;
    use open_arrow_access_test_support::temp_fs;
    let snapshot = oai_snapshot(&["2410.00001"]).await;
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
