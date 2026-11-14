use crate::{Error, ErrorKind};

pub(super) fn source_state_unavailable() -> Error {
    Error::new(
        ErrorKind::Io,
        "file_source_reader",
        "file source state is poisoned",
    )
}

pub(super) fn schema_changed() -> Error {
    Error::new(
        ErrorKind::Decode,
        "file_source_reader",
        "file schema differs from the schema fixed at open",
    )
}
