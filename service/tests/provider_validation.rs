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

#[test]
fn optional_representations_keep_selector_order_and_reject_invalid_links() {
    use open_arrow_access_service::common::optional_links;
    let raw = serde_json::json!({"pdf":"https://files.example/paper?sig=a%2Bb","text":null,"other":"retained"});
    assert_eq!(
        optional_links(
            &raw,
            &[("text", "text"), ("pdf", "paper"), ("missing", "none")]
        )
        .unwrap(),
        vec![(
            "paper".into(),
            "https://files.example/paper?sig=a%2Bb".into()
        )]
    );
    assert!(optional_links(&serde_json::json!({"pdf":123}), &[("pdf", "paper")]).is_err());
    assert!(
        optional_links(
            &serde_json::json!({"pdf":"file:///private"}),
            &[("pdf", "paper")]
        )
        .is_err()
    );
}
