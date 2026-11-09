pub(super) use crate::common::{decode, invalid};

pub(super) fn url(error: url::ParseError) -> open_arrow_access_core::Error {
    invalid("invalid LoC metadata URL").with_source(error)
}

pub(super) fn scheme(_: ()) -> open_arrow_access_core::Error {
    invalid("invalid LoC metadata URL scheme")
}
