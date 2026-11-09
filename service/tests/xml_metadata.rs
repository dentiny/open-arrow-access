use open_arrow_access_service::xml_metadata::validate_root;
use open_arrow_access_test_support::fixture;

#[tokio::test]
async fn checks_namespaced_envelopes_and_accepts_standard_xml_headers() {
    let manifest = fixture!("xml-metadata", "namespaced-header.xml").await;
    validate_root(&manifest, "manifest").unwrap();
    let oai = fixture!("xml-metadata", "oai.xml").await;
    validate_root(&oai, "OAI-PMH").unwrap();
    for name in ["wrong-root.xml", "empty.xml"] {
        let raw = fixture!("xml-metadata", name).await;
        assert!(validate_root(&raw, "manifest").is_err(), "{name}");
    }
}

#[tokio::test]
async fn document_uses_xml_engine_for_namespaces_entities_and_complete_input() {
    use arrow_array::cast::AsArray;
    use arrow_schema::DataType::Utf8;
    use open_arrow_access_file_format::xml::XmlField;
    use open_arrow_access_service::xml_metadata::XmlDocument;

    let raw = fixture!("xml-metadata", "namespaces-entities.xml").await;
    let document = XmlDocument::new(&raw, "manifest").unwrap();
    let batch = document
        .read(vec![XmlField::new(
            "title",
            document.element("title"),
            Utf8,
            false,
        )])
        .await
        .unwrap();
    assert_eq!(batch.column(0).as_string::<i32>().value(0), "A & B");
    assert!(raw.windows(9).any(|part| part == b"<!DOCTYPE"));

    for name in ["multiple-roots.xml", "truncated.xml", "trailing-entity.xml"] {
        let raw = fixture!("xml-metadata", name).await;
        let document = XmlDocument::new(&raw, "manifest").unwrap();
        assert!(document.read(vec![]).await.is_err(), "{name}");
    }
    let raw = fixture!("xml-metadata", "internal-dtd.xml").await;
    assert!(XmlDocument::new(&raw, "manifest").is_err());
}

#[test]
fn rejects_invalid_utf8_in_xml_metadata_headers() {
    for bytes in [
        b"<mani\xfffest/>".as_slice(),
        b"<manifest xmlns=\"urn:\xff\"/>".as_slice(),
        b"<!DOCTYPE manifest SYSTEM \"\xff\"><manifest/>".as_slice(),
    ] {
        let error = validate_root(bytes, "manifest").unwrap_err();
        assert_eq!(error.kind(), open_arrow_access_core::ErrorKind::Decode);
    }
}
