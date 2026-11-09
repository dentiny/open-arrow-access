use open_arrow_access_service::common::{array, download_url, filename, segment, text};
#[test]
fn validates_download_origins_without_changing_signed_queries() {
    let signed = "https://files.example/path%20one?sig=a%2Bb%2Fz";
    assert_eq!(download_url(signed).unwrap().as_str(), signed);
    for url in [
        "file:///tmp/x",
        "https://u:p@example.com/x",
        "https://example.com/x#fragment",
    ] {
        assert!(download_url(url).is_err());
    }
}
#[test]
fn shares_json_field_and_component_rules() {
    let value = serde_json::json!({"name":"a","files":[],"null":null});
    assert_eq!(text(&value, "name").unwrap(), "a");
    assert!(array(&value, "files").unwrap().is_empty());
    assert!(text(&value, "null").is_err());
    assert!(array(&value, "missing").is_err());
    assert_eq!(segment("org/repo").unwrap(), "org%2Frepo");
    assert_eq!(filename("paper #1%.pdf").unwrap(), "paper%20%231%25.pdf");
    for name in ["", "..", "../x", "a\\b", " padded "] {
        assert!(filename(name).is_err());
    }
}
