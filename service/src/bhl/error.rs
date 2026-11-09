pub(super) use crate::common::{decode, invalid};

pub(super) fn unauthorized() -> open_arrow_access_core::Error {
    open_arrow_access_core::Error::new(
        open_arrow_access_core::ErrorKind::PermissionDenied,
        "resolve_bhl",
        "BHL API key is unauthorized",
    )
}
