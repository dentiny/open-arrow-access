use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn invalid_manifest_url() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "configure_commoncrawl_manifest",
        "expected an HTTP(S) manifest file URL",
    )
}
