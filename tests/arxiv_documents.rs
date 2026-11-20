use arrow_array::cast::AsArray;
use open_arrow_access::{
    Accessor, Bytes, ObjectRef, bytes_reader,
    crawl::{CrawlAccessor, CrawlOptions},
    service::arxiv::{
        ArxivArticle, ArxivArticleFormat, ArxivArticleReadRequest, ArxivService,
        ArxivSourceOptions, ArxivSourcePackaging, open_source_package,
    },
    storage::OpenDalAccessor,
};
use open_arrow_access_test_support::{fixture, fixture_path, fs_operator};

#[tokio::test]
async fn crawl_preserves_original_formats_and_source_ancillary_files() {
    let articles = ArxivService::new(OpenDalAccessor::new(fs_operator(fixture_path!(
        "arxiv-articles",
        ""
    ))))
    .article_records();
    let crawl = CrawlAccessor::new(articles, CrawlOptions::default());
    let mut reader = crawl
        .open(ArxivArticleReadRequest::new(vec![
            ArxivArticle::new("2410.00001v1", ArxivArticleFormat::Pdf),
            ArxivArticle::new("2410.00001v1", ArxivArticleFormat::Latex),
            ArxivArticle::new("2410.00001v1", ArxivArticleFormat::Html),
        ]))
        .await
        .unwrap();
    let mut formats = Vec::new();
    let mut source = None;
    while let Some(batch) = reader.next().await.unwrap() {
        for row in 0..batch.num_rows() {
            assert_eq!(
                batch["article_id"].as_string::<i64>().value(row),
                "2410.00001v1"
            );
            let format = batch["format"].as_string::<i64>().value(row);
            let path = match format {
                "pdf" => "paper.pdf",
                "latex" => "source.tar.gz",
                "html" => "paper.html",
                other => panic!("unexpected format {other}"),
            };
            let bytes = batch["data"].as_binary::<i64>().value(row);
            assert_eq!(bytes, fixture!("arxiv-articles", path).await);
            if format == "latex" {
                source = Some(bytes.to_vec());
            }
            formats.push(format.to_owned());
        }
    }
    formats.sort();
    assert_eq!(formats, ["html", "latex", "pdf"]);

    // Package expansion is explicit; the original source bytes remain available.
    let mut members = open_source_package(
        Box::new(bytes_reader(
            Bytes::from(source.unwrap()),
            Some(ObjectRef::new("src/2410.00001v1")),
        )),
        "2410.00001v1",
        ArxivSourcePackaging::GzipTar,
        ArxivSourceOptions::default(),
    )
    .await
    .unwrap();
    let mut ancillary = Vec::new();
    while let Some(batch) = members.next().await.unwrap() {
        for row in 0..batch.num_rows() {
            let name = batch["entry_name"].as_binary::<i64>().value(row);
            if name.starts_with(b"anc/") {
                let name = std::str::from_utf8(name).unwrap();
                assert_eq!(
                    batch["data"].as_binary::<i64>().value(row),
                    fixture!("arxiv-articles", &format!("members/{name}")).await
                );
                ancillary.push(name.to_owned());
            }
        }
    }
    assert_eq!(
        ancillary,
        [
            "anc/demo.mp4",
            "anc/data.csv",
            "anc/metadata.xml",
            "anc/archive.zip"
        ]
    );
}
