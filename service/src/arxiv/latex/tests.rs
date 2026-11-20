use super::*;
use arrow_array::{ArrayRef, StringArray, types::UInt64Type};
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

fn metadata(id: &str) -> ArxivMetadataIndex {
    let batch = RecordBatch::try_from_iter(vec![(
        "identifier",
        Arc::new(StringArray::from(vec![id])) as ArrayRef,
    )])
    .unwrap();
    ArxivMetadataIndex::try_new(batch, "identifier").unwrap()
}

#[tokio::test]
async fn packages_keep_only_tex_with_provenance_and_metadata() {
    let original = fixture!("arxiv-articles", "members/main.tex").await;
    for file in ["source.tar", "source.tar.gz"] {
        let data = fixture!("arxiv-articles", file).await;
        let batches = read("2410/2410.00001v2.gz", &data).await.unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert!(batches.iter().all(|batch| batch.num_rows() == 1));
        let batch = arrow_select::concat::concat_batches(&latex_schema(), &batches).unwrap();
        assert_eq!(
            batch["text"].as_string::<i64>().value(0).as_bytes(),
            original
        );
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
        let joined = metadata("oai:arXiv.org:2410.00001")
            .attach_articles(batch.clone())
            .unwrap();
        assert_eq!(
            joined["metadata"]
                .as_struct()
                .column(0)
                .as_string::<i32>()
                .value(0),
            "oai:arXiv.org:2410.00001"
        );
        assert!(
            metadata("2410.99999")
                .attach_articles(batch.clone())
                .is_err()
        );
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
        assert!(!is_tex(name).unwrap());
    }
    assert!(is_tex(b"chapters/intro.tex").unwrap());
    assert!(is_tex(b"../escape.tex").is_err());
}
