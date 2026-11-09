use open_arrow_access_service::xml_metadata::validate_root;

#[test]
fn checks_namespaced_envelopes_and_accepts_standard_xml_headers() {
    validate_root(b"<?xml version=\"1.0\"?><!-- header --><!DOCTYPE manifest SYSTEM \"manifest.dtd\"><m:manifest xmlns:m=\"urn:meca\"/>","manifest").unwrap();
    validate_root(b"<OAI-PMH><ListRecords/></OAI-PMH>", "OAI-PMH").unwrap();
    assert!(validate_root(b"<html/>", "manifest").is_err());
    assert!(validate_root(b"", "manifest").is_err());
}

#[tokio::test]
async fn document_uses_xml_engine_for_namespaces_entities_and_complete_input() {
    use arrow_array::cast::AsArray;
    use arrow_schema::DataType::Utf8;
    use open_arrow_access_service::xml_metadata::XmlDocument;
    use xml_arrow::XmlField;

    let raw = b"<?xml version=\"1.0\"?><!DOCTYPE manifest SYSTEM \"https://invalid.example/never-fetch.dtd\"><x:manifest xmlns:x=\"urn:meca\"><x:title>A &amp; B</x:title></x:manifest>";
    let document = XmlDocument::new(raw, "manifest").unwrap();
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

    for raw in [
        b"<manifest/><manifest/>".as_slice(),
        b"<manifest>truncated",
        b"<manifest/>&unknown;",
    ] {
        let document = XmlDocument::new(raw, "manifest").unwrap();
        assert!(document.read(vec![]).await.is_err());
    }
    assert!(
        XmlDocument::new(
            b"<!DOCTYPE manifest [<!ENTITY custom 'value'>]><manifest/>",
            "manifest"
        )
        .is_err()
    );
}
