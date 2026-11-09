//! Exercise provider outputs through real format/archive readers, entirely offline.
mod providers;
use arrow_array::{Array, cast::AsArray};
use arrow_schema::DataType;
use http::StatusCode;
use open_arrow_access_core::{Accessor, ReadRequest, ReaderFactoryExt, read_all};
use open_arrow_access_decompression::zip::ZipLimits;
use open_arrow_access_file_format::xml::{XmlField, XmlFormat, XmlSchema};
use open_arrow_access_service::{
    govinfo::GovInfoService, meca::MecaArchive, xml_metadata::XmlDocument,
};
use open_arrow_access_test_support::{MemoryAccessor, fixture, http::HttpFixture};

#[tokio::test]
async fn govinfo_bulk_download_preserves_and_decodes_a_real_issue() {
    let http = HttpFixture::new();
    let original = fixture!("govinfo", "fr-2026-01-02.xml").await;
    http.respond(
        "/bulkdata/json/FR/2026/01",
        StatusCode::OK,
        fixture!("govinfo", "issue-catalog.json").await,
    );
    http.file("/bulkdata/FR/2026/01/FR-2026-01-02.xml", original.clone());
    let service = GovInfoService::with_connections(
        providers::connection(&http),
        providers::connection(&http),
    );
    let files = service.bulk_directory("FR/2026/01").await.unwrap();
    assert_eq!(
        http.requests().len(),
        1,
        "resolution must not fetch content"
    );
    let reader = files.open("FR-2026-01-02.xml".into()).await.unwrap();
    drop(files);
    drop(service);
    let downloaded = read_all(reader, "read_govinfo").await.unwrap();
    assert_eq!(downloaded.as_slice(), original.as_slice());
    let batch = XmlDocument::new(&downloaded, "FEDREG")
        .unwrap()
        .read(vec![
            XmlField::list(
                "volumes",
                ".",
                false,
                XmlField::new("volume", "VOL", DataType::Utf8, false),
            ),
            XmlField::list(
                "numbers",
                ".",
                false,
                XmlField::new("number", "NO", DataType::Utf8, false),
            ),
        ])
        .await
        .unwrap();
    assert_eq!(batch.num_rows(), 1);
    for (field, expected) in [("volumes", "91"), ("numbers", "1")] {
        let values = batch[field].as_list::<i32>().value(0);
        let values = values.as_string::<i32>();
        assert!(values.len() > 1, "the real issue repeats these elements");
        assert!(values.iter().all(|value| value == Some(expected)));
    }
    assert_eq!(http.requests().len(), 2);
}

#[tokio::test]
async fn meca_selected_xml_composes_with_the_xml_reader() {
    let data = fixture!("meca", "valid.meca").await;
    let input = MemoryAccessor::new(&data, 7)
        .with_ranges()
        .open("paper.meca".into())
        .await
        .unwrap();
    let archive = MecaArchive::open(input, ZipLimits::default())
        .await
        .unwrap();
    let file = archive
        .files()
        .iter()
        .find(|file| file.media_type.as_deref() == Some("application/xml"))
        .unwrap();
    let input = archive
        .archive()
        .open_entry(file.entry_index, std::num::NonZeroUsize::new(3).unwrap())
        .await
        .unwrap();
    let format = XmlFormat::new("/article").with_schema(
        XmlSchema::new(vec![XmlField::new("content", ".", DataType::Utf8, false)]).unwrap(),
    );
    let mut reader = format
        .open_input(input, ReadRequest::new("content/article.xml"))
        .await
        .unwrap();
    drop(archive);
    let schema = reader.metadata().schema.clone();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.schema(), schema);
    assert_eq!(batch["content"].as_string::<i32>().value(0), "original");
    assert!(reader.next().await.unwrap().is_none());
}
