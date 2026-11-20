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
        let batch = arrow_select::concat::concat_batches(&payload_schema(), &batches).unwrap();
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
    let snapshot = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(arrow_array::StringArray::from(vec![
                "oai:arXiv.org:2410.00001",
            ])) as arrow_array::ArrayRef,
        ),
        (
            "arxiv:license",
            Arc::new(arrow_array::StringArray::from(vec![Some(
                "http://creativecommons.org/licenses/by/4.0/",
            )])) as arrow_array::ArrayRef,
        ),
        (
            "extra",
            Arc::new(arrow_array::StructArray::from(
                RecordBatch::try_from_iter(vec![
                    (
                        "title",
                        Arc::new(arrow_array::StringArray::from(vec!["Full title"]))
                            as arrow_array::ArrayRef,
                    ),
                    (
                        "optional",
                        Arc::new(arrow_array::StringArray::from(vec![None::<&str>]))
                            as arrow_array::ArrayRef,
                    ),
                ])
                .unwrap(),
            )) as arrow_array::ArrayRef,
        ),
    ])
    .unwrap();
    let metadata = Arc::new(ArxivMetadataIndex::try_new(snapshot.clone(), "id").unwrap());
    let schema = content_schema(&metadata);
    assert_eq!(schema.fields().len(), 10);
    assert!(!schema.field_with_name("metadata").unwrap().is_nullable());
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
        let records = crate::arxiv::ArxivService::new(OpenDalAccessor::new(operator.clone()))
            .records()
            .content(format, metadata.clone());
        let mut reader = records.open(ArxivReadRequest::new(kind)).await.unwrap();
        assert_eq!(reader.metadata().schema, schema);
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.schema(), schema);
        assert_eq!(
            batch["metadata"].as_struct(),
            &arrow_array::StructArray::from(snapshot.clone())
        );
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
        let projected =
            records
                .open(ArxivReadRequest::new(kind).with_options(
                    ReadOptions::default().with_projection(["metadata", "data_type"]),
                ))
                .await
                .unwrap()
                .next()
                .await
                .unwrap()
                .unwrap();
        assert_eq!(projected.num_columns(), 2);
        assert_eq!(
            projected.column_by_name("metadata"),
            batch.column_by_name("metadata")
        );

        let empty = Arc::new(ArxivMetadataIndex::try_new(snapshot.slice(0, 0), "id").unwrap());
        let missing = crate::arxiv::ArxivService::new(OpenDalAccessor::new(operator))
            .records()
            .content(format, empty);
        let mut reader = missing
            .open(
                ArxivReadRequest::new(kind)
                    .with_options(ReadOptions::default().with_projection(["data"])),
            )
            .await
            .unwrap();
        assert!(
            reader
                .next()
                .await
                .unwrap_err()
                .to_string()
                .contains("no matching metadata")
        );
    }
}
