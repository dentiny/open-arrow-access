use super::*;
use crate::arxiv::ArxivKind;
use crate::common::Connection;
use arrow_array::{Array, StructArray, types::UInt64Type};
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{fixture, http::HttpFixture, temp_fs};

type TestContent = ArxivContentRecords<OpenDalAccessor>;

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
    let article_id = validation::content_article_id(name.as_bytes(), ArxivContentFormat::Latex)?;
    crate::arxiv::latex::read(
        raw_batch(name, data)?,
        article_id,
        ReadOptions::default().with_batch_size(std::num::NonZeroUsize::MIN),
    )
    .await?
    .try_collect()
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

#[rstest::rstest]
#[case::uncompressed("main.tex")]
#[case::gzip("main.tex.gz")]
#[tokio::test]
async fn accepts_legacy_single_latex_sources(#[case] file: &str) {
    let batches = read("hep-th/9901001.gz", &fixture!("arxiv-articles", file).await)
        .await
        .unwrap();
    assert_eq!(
        batches[0]["article_id"].as_string::<i64>().value(0),
        "hep-th9901001"
    );
}

#[tokio::test]
async fn accepts_legacy_plain_tex_and_preserves_content() {
    let data = fixture!("arxiv-articles", "plain-tex.tex").await;
    let batches = read("hep-th/9901001.gz", &data).await.unwrap();
    assert_eq!(
        batches[0]["article_id"].as_string::<i64>().value(0),
        "hep-th9901001"
    );
    assert_eq!(batches[0]["data"].as_binary::<i64>().value(0), data);
}

#[rstest::rstest]
#[case::pdf(b"%PDF-1.7")]
#[case::postscript(b"%!PS")]
#[case::dvi(b"\xf7\x02DVI content")]
#[tokio::test]
async fn non_tex_document_signatures_report_missing_tex(#[case] data: &[u8]) {
    assert_eq!(
        read("2410/2410.00001.gz", data).await.unwrap_err().kind(),
        open_arrow_access_core::ErrorKind::NotFound
    );
}

#[rstest::rstest]
#[case::pdf("arxiv-articles", "paper.pdf")]
#[case::gzip_pdf("arxiv-articles", "paper.pdf.gz")]
#[case::archive_without_tex("arxiv-content", "no-latex.tar")]
#[tokio::test]
async fn missing_tex_errors_include_article_provenance(#[case] group: &str, #[case] file: &str) {
    let error = read("2410/2410.00001.gz", &fixture!(group, file).await)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), open_arrow_access_core::ErrorKind::NotFound);
    assert_eq!(
        error.object().unwrap().locator(),
        "src/arXiv_src_2410_001.tar#entry=7"
    );
    assert!(error.to_string().contains("2410.00001"));
}

#[tokio::test]
async fn corrupt_gzip_sources_report_decode_errors() {
    let corrupt = fixture!("arxiv-articles", "bad-crc.tar.gz").await;
    assert_eq!(
        read("2410/2410.00001.gz", &corrupt)
            .await
            .unwrap_err()
            .kind(),
        open_arrow_access_core::ErrorKind::Decode,
    );
}

#[rstest::rstest]
#[case::ancillary(b"anc/extra.tex")]
#[case::nested_ancillary(b"nested/ANC/extra.tex")]
#[case::image(b"figure.png")]
#[test]
fn tex_selection_excludes_ancillary_and_non_tex_files(#[case] name: &[u8]) {
    assert!(!validation::is_tex(name).unwrap());
}

#[test]
fn tex_selection_accepts_nested_tex_files() {
    assert!(validation::is_tex(b"chapters/intro.tex").unwrap());
}

#[test]
fn tex_selection_rejects_path_traversal() {
    assert!(validation::is_tex(b"../escape.tex").is_err());
}

#[tokio::test]
async fn latex_skips_linked_attachments_and_keeps_following_tex() {
    let data = fixture!("tar", "links-and-files.tar").await;
    let batches = read("2509/2509.17155.gz", &data).await.unwrap();
    assert_eq!(batches.len(), 2);
    for (batch, index, name, body) in [
        (&batches[0], 0, b"main.tex".as_slice(), b"first".as_slice()),
        (&batches[1], 3, b"last.tex".as_slice(), b"last".as_slice()),
    ] {
        assert_eq!(
            batch["source_index"].as_primitive::<UInt64Type>().value(0),
            index
        );
        assert_eq!(batch["source_name"].as_binary::<i64>().value(0), name);
        assert_eq!(batch["data"].as_binary::<i64>().value(0), body);
    }
}

#[tokio::test]
async fn packages_preserve_non_utf8_tex_names_and_payloads() {
    let mut data = fixture!("arxiv-articles", "source.tar").await;
    // Mutate a real regular-file header without transcoding the stored path.
    let header = &mut data[512..1024];
    header[0] = 0xff;
    header[148..156].fill(b' ');
    let checksum: u32 = header.iter().map(|byte| u32::from(*byte)).sum();
    header[148..156].copy_from_slice(format!("{checksum:06o}\0 ").as_bytes());
    let batches = read("2410/2410.00001.gz", &data).await.unwrap();
    assert_eq!(
        batches[0]["source_name"].as_binary::<i64>().value(0),
        b"\xffain.tex"
    );
    assert_eq!(
        batches[0]["data"].as_binary::<i64>().value(0),
        fixture!("arxiv-articles", "members/main.tex").await
    );
}

#[rstest::rstest]
#[case(b"anc/\xff.tex", false)]
#[case(b"nested/ANC/\xff.tex", false)]
#[case(b"chapters/\xff.TEX", true)]
fn tex_selection_handles_binary_paths(#[case] name: &[u8], #[case] selected: bool) {
    assert_eq!(validation::is_tex(name).unwrap(), selected);
}

#[rstest::rstest]
#[case(b"../\xff.tex")]
#[case(b"/\xff.tex")]
#[case(b"nested/../\xff.tex")]
#[case(b"nested\\\xff.tex")]
fn binary_tex_names_still_reject_unsafe_paths(#[case] name: &[u8]) {
    assert!(validation::is_tex(name).is_err());
}

#[tokio::test]
async fn formats_preserve_content_and_join_metadata() {
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
        let http = HttpFixture::new();
        let (_temp, records, key) = content_fixture(format, kind, fixture_name, &http).await;
        let mut reader = records.open(ArxivReadRequest::new(kind)).await.unwrap();
        assert_eq!(reader.metadata().schema, schema);
        assert!(http.requests().is_empty());
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.schema(), schema);
        assert_metadata(&batch, &snapshot, 1);
        assert_content(&batch, format).await;
        assert!(reader.next().await.unwrap().is_none());
        let request = ArxivReadRequest::new(kind).with_month(2410);
        // Reordered and empty projections still preserve row counts.
        for columns in [vec!["metadata".into(), "article_id".into()], vec![]] {
            let mut request = request.clone();
            request.options.projection = Some(columns.clone());
            let batch = read_one(&records, request).await;
            assert_eq!(batch.num_columns(), columns.len());
            if !columns.is_empty() {
                assert_metadata(&batch, &snapshot, 1);
            }
        }
        assert_eq!(http.requests().len(), 1); // Reopens and projections use the cache.
        let records = records.with_metadata(index(snapshot.clone())).unwrap();
        assert_metadata(&read_one(&records, request).await, &snapshot, 1);
        assert_eq!(http.requests().len(), 1); // Snapshots bypass OAI.
        // Missing matches fail even when metadata is projected away.
        let records = records.with_metadata(index(snapshot.slice(2, 1))).unwrap();
        let mut request = ArxivReadRequest::new(kind).with_shard(key);
        request.options.projection = Some(vec![]);
        let mut reader = records.open(request).await.unwrap();
        assert!(reader.next().await.is_err());
    }
}

async fn content_fixture(
    format: ArxivContentFormat,
    kind: ArxivKind,
    fixture_name: &str,
    http: &HttpFixture,
) -> (tempfile::TempDir, TestContent, String) {
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
    let (temp, operator) = temp_fs(&[(&manifest_key, manifest.as_bytes()), (&key, &bytes)]);
    let connection = metadata_connection(http, "2410.00001", "metadata-record.xml").await;
    let records = crate::arxiv::ArxivService::new(OpenDalAccessor::new(operator))
        .with_metadata_connection(connection)
        .records()
        .content(format);
    (temp, records, key)
}

async fn read_one(records: &TestContent, request: ArxivReadRequest) -> RecordBatch {
    let mut reader = records.open(request).await.unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.schema(), reader.metadata().schema);
    assert_eq!(batch.num_rows(), 1);
    assert!(reader.next().await.unwrap().is_none());
    batch
}

async fn assert_content(batch: &RecordBatch, format: ArxivContentFormat) {
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
}

async fn metadata_connection(http: &HttpFixture, article: &str, file: &str) -> Connection {
    let identifier = super::super::id::oai_identifier(article).unwrap();
    let encoded: String = url::form_urlencoded::byte_serialize(identifier.as_bytes()).collect();
    let path = format!("/oai?verb=GetRecord&metadataPrefix=arXivRaw&identifier={encoded}");
    http.respond(
        &path,
        http::StatusCode::OK,
        fixture!("arxiv-content", file).await,
    );
    Connection::new(http.endpoint())
        .unwrap()
        .with_context(http.context())
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
    let expected = arrow_cast::cast(
        &StructArray::from(snapshot.clone()).slice(expected_row, 1),
        content_schema()
            .field_with_name("metadata")
            .unwrap()
            .data_type(),
    )
    .unwrap();
    let expected = expected.as_struct();
    let metadata = batch["metadata"].as_struct();
    assert_eq!(metadata.null_count(), 0);
    for row in 0..batch.num_rows() {
        assert_eq!(&metadata.slice(row, 1), expected);
    }
}

#[tokio::test]
async fn metadata_normalizes_legacy_ids_repeats_tex_rows_and_retains_deletions() {
    let snapshot = oai_snapshot().await;
    let http = HttpFixture::new();
    let resolver = super::super::metadata_fetch::MetadataResolver::new(
        metadata_connection(&http, "hep-th9901001", "metadata-legacy.xml").await,
    );
    let (index, same) = futures::try_join!(
        resolver.get("hep-th9901001"),
        resolver.get("hep-th/9901001")
    )
    .unwrap();
    assert!(Arc::ptr_eq(&index, &same));
    assert_eq!(http.requests().len(), 1);
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
        "2410.99999",
    )
    .unwrap();
    let resolver = super::super::metadata_fetch::MetadataResolver::new(
        metadata_connection(&http, "2410.99999", "metadata-deleted.xml").await,
    );
    let index = resolver.get("2410.99999").await.unwrap();
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

#[tokio::test]
async fn default_metadata_propagates_failures_even_without_projected_metadata() {
    for file in [
        "metadata-missing.xml",
        "metadata-empty.xml",
        "metadata-incomplete.xml",
        "metadata-legacy.xml",
    ] {
        let http = HttpFixture::new();
        let connection = metadata_connection(&http, "2410.00001", file).await;
        let bytes = fixture!("arxiv-content", "pdf.tar").await;
        let (_temp, operator) = temp_fs(&[("pdf/one.tar", &bytes)]);
        let records = crate::arxiv::ArxivService::new(OpenDalAccessor::new(operator))
            .with_metadata_connection(connection)
            .records()
            .content(ArxivContentFormat::Pdf);
        let mut request = ArxivReadRequest::new(ArxivKind::Pdf).with_shard("pdf/one.tar");
        request.options.projection = Some(vec![]);
        let mut reader = records.open(request).await.unwrap();
        assert!(reader.next().await.is_err(), "{file}");
        assert_eq!(http.requests().len(), 1);
    }
}

#[tokio::test]
async fn explicit_missing_policy_skips_pdf_articles_and_keeps_latex() {
    use open_arrow_access_core::AccessorExt;
    use open_arrow_access_layers::{MissingObjectLayer, MissingObjectPolicy};
    use std::sync::Mutex;

    let http = HttpFixture::new();
    let connection = metadata_connection(&http, "2410.00001", "metadata-record.xml").await;
    metadata_connection(&http, "2410.00002", "metadata-second.xml").await;
    let bytes = fixture!("arxiv-content", "mixed-source.tar").await;
    let (_temp, operator) = temp_fs(&[("src/mixed.tar", &bytes)]);
    let records = crate::arxiv::ArxivService::new(OpenDalAccessor::new(operator))
        .with_metadata_connection(connection)
        .records()
        .content(ArxivContentFormat::Latex);
    let request = ArxivReadRequest::new(ArxivKind::Source).with_shard("src/mixed.tar");
    let mut strict = records.open(request.clone()).await.unwrap();
    assert_eq!(
        strict.next().await.unwrap_err().kind(),
        open_arrow_access_core::ErrorKind::NotFound
    );

    let skipped = Arc::new(Mutex::new(Vec::new()));
    let observed = skipped.clone();
    let records = records.layer(MissingObjectLayer::new(
        MissingObjectPolicy::Skip,
        move |error| {
            observed
                .lock()
                .unwrap()
                .push(error.object().unwrap().locator().to_owned());
            Ok(())
        },
    ));
    let mut reader = records.open(request).await.unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch["article_id"].as_string::<i64>().value(0),
        "2410.00002"
    );
    assert_eq!(
        batch["data"].as_binary::<i64>().value(0),
        fixture!("arxiv-articles", "main.tex").await
    );
    assert_eq!(batch["metadata"].null_count(), 0);
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(*skipped.lock().unwrap(), ["src/mixed.tar#entry=0"]);
}

#[tokio::test]
async fn missing_latex_in_multiple_tar_selection_cannot_skip_entire_shards() {
    use open_arrow_access_core::AccessorExt;
    use open_arrow_access_layers::{MissingObjectLayer, MissingObjectPolicy};
    let http = HttpFixture::new();
    let connection = metadata_connection(&http, "2410.00001", "metadata-record.xml").await;
    let bytes = fixture!("arxiv-content", "mixed-source.tar").await;
    let manifest = fixture!("arxiv-content", "source-manifest.xml").await;
    let (_temp, operator) = temp_fs(&[
        ("src/arXiv_src_manifest.xml", &manifest),
        ("src/arXiv_src_2411_001.tar", &bytes),
        ("src/arXiv_src_2411_002.tar", &bytes),
    ]);
    let records = crate::arxiv::ArxivService::new(OpenDalAccessor::new(operator))
        .with_metadata_connection(connection)
        .records()
        .content(ArxivContentFormat::Latex)
        .layer(MissingObjectLayer::new(MissingObjectPolicy::Skip, |_| {
            panic!("must not discard an entire shard")
        }));
    let mut reader = records
        .open(ArxivReadRequest::new(ArxivKind::Source).with_month(2411))
        .await
        .unwrap();
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), open_arrow_access_core::ErrorKind::Decode);
    assert!(error.to_string().contains("article-level skipping"));
}
