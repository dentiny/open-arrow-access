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

pub(super) fn parquet_input_error() -> Error {
    Error::new(
        ErrorKind::Unsupported,
        "open_parquet",
        "Parquet requires range reads and a known byte length; materialize sequential input first",
    )
}

pub(super) fn decode_error<E>(operation: &'static str, error: E) -> Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    crate::error::source_error(operation, ErrorKind::Decode, error)
}

pub(super) fn external_error(error: Error) -> parquet::errors::ParquetError {
    parquet::errors::ParquetError::External(Box::new(error))
}

pub(super) fn unknown_input_length() -> parquet::errors::ParquetError {
    parquet::errors::ParquetError::General("input length is unknown".to_owned())
}
