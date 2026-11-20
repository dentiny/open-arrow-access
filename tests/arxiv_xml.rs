#![cfg(feature = "format-xml")]

use arrow_array::cast::AsArray;
use open_arrow_access::{
    Accessor, AttachmentFormat, AttachmentOptions, EmissionPolicy,
    crawl::{CrawlAccessor, CrawlOptions},
    dataset::file_set::FileSetRequest,
    storage::OpenDalAccessor,
};
use open_arrow_access_test_support::{fixture, fixture_path, fs_operator};

fn crawl(format: AttachmentFormat) -> CrawlAccessor<FileSetRequest> {
    CrawlAccessor::attachments(
        OpenDalAccessor::new(fs_operator(fixture_path!("arxiv-xml", ""))),
        AttachmentOptions::new(format),
        CrawlOptions::default().with_emission(EmissionPolicy::Ordered),
    )
    .unwrap()
}

#[tokio::test]
async fn xml_original_bytes_survive_the_crawl() {
    let mut reader = crawl(AttachmentFormat::Binary)
        .open(FileSetRequest::new(["article.xml"]))
        .await
        .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch["data"].as_binary::<i64>().value(0),
        fixture!("arxiv-xml", "article.xml").await
    );
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn xml_attachment_uses_the_existing_structured_reader() {
    let mut reader = crawl(AttachmentFormat::Xml {
        record_path: "/article".into(),
    })
    .open(FileSetRequest::new(["article.xml"]))
    .await
    .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch["id"].as_string::<i32>().value(0), "2410.00001v1");
    assert_eq!(batch["title"].as_string::<i32>().value(0), "Original paper");
    assert!(reader.next().await.unwrap().is_none());
}
