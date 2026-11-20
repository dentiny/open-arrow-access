use arrow_array::cast::AsArray;
use open_arrow_access::{
    Accessor,
    crawl::{CrawlAccessor, CrawlOptions},
    service::arxiv::{ArxivArticle, ArxivArticleFormat, ArxivArticleReadRequest, ArxivService},
    storage::OpenDalAccessor,
};
use open_arrow_access_test_support::{fixture, fixture_path, fs_operator};

#[tokio::test]
async fn crawl_preserves_original_formats() {
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
            formats.push(format.to_owned());
        }
    }
    formats.sort();
    assert_eq!(formats, ["html", "latex", "pdf"]);
}
