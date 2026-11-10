use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn parquet_partition_input_error() -> Error {
    Error::new(
        ErrorKind::Unsupported,
        "plan_parquet",
        "Parquet partitions require range reads and a known byte length",
    )
}

pub(super) fn parquet_partition_changed() -> Error {
    Error::new(
        ErrorKind::Conflict,
        "open_parquet_partition",
        "input length or version changed after row-group planning",
    )
}
