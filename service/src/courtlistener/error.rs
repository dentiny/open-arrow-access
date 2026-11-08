use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn invalid() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "courtlistener_snapshot",
        "expected a lowercase table name and a valid YYYY-MM-DD date (bulk-data/<table>-<date>.csv.bz2)",
    )
}
