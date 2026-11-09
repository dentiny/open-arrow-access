use open_arrow_access_service::xml_metadata::validate_root;
#[test]
fn checks_namespaced_envelopes_and_accepts_standard_xml_headers() {
    validate_root(b"<?xml version=\"1.0\"?><!-- header --><!DOCTYPE manifest SYSTEM \"manifest.dtd\"><m:manifest xmlns:m=\"urn:meca\"/>","manifest").unwrap();
    validate_root(b"<OAI-PMH><ListRecords/></OAI-PMH>", "OAI-PMH").unwrap();
    assert!(validate_root(b"<html/>", "manifest").is_err());
    assert!(validate_root(b"", "manifest").is_err());
}
