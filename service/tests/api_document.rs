use open_arrow_access_core::{Bytes, ErrorKind};
use open_arrow_access_service::common::ApiDocument;

#[test]
fn malformed_json_is_a_decode_error() {
    assert_eq!(
        ApiDocument::from_bytes(Bytes::from_static(b"{"))
            .unwrap_err()
            .kind(),
        ErrorKind::Decode
    );
}
