use open_arrow_access_service::common::{array, filename, resolve_download_url, segment, text};
#[test]
fn validates_download_origins_without_changing_signed_queries() {
    let signed = "https://files.example/path%20one?sig=a%2Bb%2Fz";
    assert_eq!(resolve_download_url(signed).unwrap().as_str(), signed);
    for url in [
        "file:///tmp/x",
        "https://u:p@example.com/x",
        "https://example.com/x#fragment",
    ] {
        assert!(resolve_download_url(url).is_err());
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
    use open_arrow_access_service::common::{LinkField, MetadataLink, resolve_optional_links};
    let raw = serde_json::json!({"pdf":"https://files.example/paper?sig=a%2Bb","text":null,"other":"retained"});
    assert_eq!(
        resolve_optional_links(
            &raw,
            &[
                LinkField {
                    json_field: "text",
                    file_name: "text"
                },
                LinkField {
                    json_field: "pdf",
                    file_name: "paper"
                },
                LinkField {
                    json_field: "missing",
                    file_name: "none"
                }
            ]
        )
        .unwrap(),
        vec![MetadataLink {
            file_name: "paper".into(),
            url: "https://files.example/paper?sig=a%2Bb".into(),
        }]
    );
    assert!(
        resolve_optional_links(
            &serde_json::json!({"pdf":123}),
            &[LinkField {
                json_field: "pdf",
                file_name: "paper"
            }]
        )
        .is_err()
    );
    assert!(
        resolve_optional_links(
            &serde_json::json!({"pdf":"file:///private"}),
            &[LinkField {
                json_field: "pdf",
                file_name: "paper"
            }]
        )
        .is_err()
    );
}
