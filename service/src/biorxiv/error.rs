pub(super) use crate::common::{decode, invalid};

pub(super) fn date(error: chrono::ParseError) -> open_arrow_access_core::Error {
    invalid("invalid calendar date").with_source(error)
}
