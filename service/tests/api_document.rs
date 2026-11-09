use open_arrow_access_core::{Bytes, ErrorKind};
use open_arrow_access_service::common::ApiDocument;
use open_arrow_access_test_support::collect_bytes;
#[tokio::test]
async fn retains_exact_raw_bytes_and_unrecognized_fields() {
    let raw = Bytes::from_static(b" { \"unknown\": true, \"license\": null } ");
    let doc = ApiDocument::from_bytes(raw.clone()).unwrap();
    assert_eq!(doc.bytes(), &raw);
    assert_eq!(doc.value()["unknown"], true);
    let mut reader = doc.reader();
    drop(doc);
    assert_eq!(reader.metadata().content_length, Some(raw.len() as u64));
    assert_eq!(collect_bytes(reader.as_mut()).await.unwrap(), raw);
    assert!(reader.next().await.unwrap().is_none());
}
#[test]
fn malformed_json_is_a_decode_error() {
    assert_eq!(
        ApiDocument::from_bytes(Bytes::from_static(b"{"))
            .unwrap_err()
            .kind(),
        ErrorKind::Decode
    );
}
