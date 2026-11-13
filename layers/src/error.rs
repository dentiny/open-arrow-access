use arrow_schema::ArrowError;
use open_arrow_access_core::{Error, ErrorKind};

fn column_error(
    kind: ErrorKind,
    operation: &'static str,
    column: &str,
    error: impl std::error::Error + Send + Sync + 'static,
) -> Error {
    Error::new(kind, operation, format!("column {column:?}: {error}")).with_source(error)
}

pub(crate) fn evolution_config_error(column: &str, error: ArrowError) -> Error {
    column_error(
        ErrorKind::InvalidInput,
        "configure_schema_evolution",
        column,
        error,
    )
}

pub(crate) fn schema_error() -> Error {
    Error::new(
        ErrorKind::Decode,
        "parallel_read",
        "partition schema differs from scan schema",
    )
}

pub(crate) fn partition_task_error(error: tokio::task::JoinError) -> Error {
    Error::new(
        ErrorKind::Decode,
        "parallel_read",
        format!("partition task failed: {error}"),
    )
}

pub(crate) fn invalid_probability() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "sample",
        "sampling probability must be finite and between 0 and 1",
    )
}

pub(crate) fn invalid_emission() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "unordered_emission",
        "source requires global record order",
    )
}

pub(crate) fn conflicting_parallel_settings() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "parallel_read",
        "conflicting parallel read settings on one scan",
    )
}

pub(crate) fn extension_cast(field: &arrow_schema::Field) -> Error {
    Error::new(
        ErrorKind::Unsupported,
        "plan_cast",
        format!(
            "column {:?}: casts involving extension fields are unsupported",
            field.name()
        ),
    )
}

pub(crate) fn unsupported_cast(
    field: &arrow_schema::Field,
    from: &arrow_schema::DataType,
    target: &arrow_schema::DataType,
) -> Error {
    column_error(
        ErrorKind::Unsupported,
        "plan_cast",
        field.name(),
        cannot_cast(from, target),
    )
}

pub(crate) fn cannot_cast(
    from: &arrow_schema::DataType,
    target: &arrow_schema::DataType,
) -> ArrowError {
    ArrowError::CastError(format!("cannot cast {from} to {target}"))
}

pub(crate) fn sample_error(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::new(ErrorKind::Decode, "sample", error.to_string()).with_source(error)
}

pub(crate) fn cast_error(
    name: &str,
    from: &arrow_schema::DataType,
    target: &arrow_schema::DataType,
    error: impl std::error::Error + Send + Sync + 'static,
) -> Error {
    Error::new(
        ErrorKind::Decode,
        "cast",
        format!("column {:?}: {} to {target}: {error}", name, from,),
    )
    .with_source(error)
}

pub(crate) fn invalid_evolution_type(data_type: &arrow_schema::DataType) -> ArrowError {
    ArrowError::InvalidArgumentError(format!("invalid or unsupported column type {data_type}"))
}

pub(crate) fn volatile_expression() -> datafusion::error::DataFusionError {
    datafusion::common::plan_datafusion_err!("volatile functions are not supported")
}

pub(crate) fn non_row_expression() -> datafusion::error::DataFusionError {
    datafusion::common::plan_datafusion_err!("only row-level expressions are supported")
}

pub(crate) fn invalid_evolution(message: &str) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "configure_schema_evolution",
        message,
    )
}
pub(crate) fn evolution_source(name: &str) -> Error {
    let error = ArrowError::SchemaError(format!("unknown field {name:?}"));
    Error::new(
        ErrorKind::InvalidInput,
        "plan_schema_evolution",
        error.to_string(),
    )
    .with_source(error)
}
pub(crate) fn evolution_output_collision() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "plan_schema_evolution",
        "evolved column names collide",
    )
}
pub(crate) fn evolution_schema_mismatch() -> Error {
    Error::new(
        ErrorKind::Decode,
        "evolve_schema",
        "source batch schema differs from the opened schema",
    )
}
pub(crate) fn invalid_added_column(name: &str, message: impl std::fmt::Display) -> Error {
    Error::new(
        ErrorKind::Decode,
        "evolve_schema",
        format!("added column {name:?}: {message}"),
    )
}
pub(crate) fn evolution_plan_error(
    name: &str,
    source: datafusion::error::DataFusionError,
) -> Error {
    column_error(
        ErrorKind::InvalidInput,
        "plan_schema_evolution",
        name,
        source,
    )
}
pub(crate) fn evolution_expression_error(
    name: &str,
    source: datafusion::error::DataFusionError,
) -> Error {
    invalid_added_column(name, &source).with_source(source)
}
pub(crate) fn evolution_expression_type(
    name: &str,
    expected: &arrow_schema::DataType,
    actual: &arrow_schema::DataType,
) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "plan_schema_evolution",
        format!("column {name:?}: expected type {expected}, expression returns {actual}"),
    )
}
pub(crate) fn evolution_batch(source: ArrowError) -> Error {
    Error::new(ErrorKind::Decode, "evolve_schema", source.to_string()).with_source(source)
}
