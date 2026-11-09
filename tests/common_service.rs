use open_arrow_access::{
    Bytes,
    service::common::{ApiDocument, Connection},
};

#[test]
fn common_service_utilities_are_available_without_provider_features() {
    // Construction validates settings but does not send a network request.
    let connection = Connection::new("https://example.com").unwrap();
    assert_eq!(connection.origin().as_str(), "https://example.com/");
    let document = ApiDocument::from_bytes(Bytes::from_static(b"{}")).unwrap();
    assert!(document.value().is_object());
}
