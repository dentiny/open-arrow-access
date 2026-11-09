pub(super) use crate::common::{decode, invalid};

pub(super) fn embargo() -> open_arrow_access_core::Error {
    open_arrow_access_core::Error::new(
        open_arrow_access_core::ErrorKind::PermissionDenied,
        "resolve_figshare",
        "article is embargoed",
    )
}
