use arrow_schema::ArrowError;
use open_arrow_access_core::{Error, ErrorKind};

fn column_error(
    kind: ErrorKind,
    operation: &'static str,
    column: &str,
    error: ArrowError,
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

pub(crate) fn cast_plan_error(column: &str, error: ArrowError) -> Error {
    column_error(ErrorKind::Unsupported, "plan_cast", column, error)
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

pub(crate) fn invalid_generated_column(column: &str, message: impl std::fmt::Display) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "plan_generated_column",
        format!("column {column:?}: {message}"),
    )
}

pub(crate) fn generated_plan_error(
    column: &str,
    error: datafusion::error::DataFusionError,
) -> Error {
    invalid_generated_column(column, &error).with_source(error)
}

pub(crate) fn generated_column_error(
    column: &str,
    error: datafusion::error::DataFusionError,
) -> Error {
    Error::new(
        ErrorKind::Decode,
        "generate_column",
        format!("column {column:?}: {error}"),
    )
    .with_source(error)
}

pub(crate) fn generated_batch_error(error: ArrowError) -> Error {
    Error::new(ErrorKind::Decode, "generate_column", error.to_string()).with_source(error)
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
    Error::new(
        ErrorKind::Unsupported,
        "plan_cast",
        format!("column {:?}: cannot cast {from} to {target}", field.name()),
    )
}

pub(crate) fn generated_schema_mismatch() -> Error {
    Error::new(
        ErrorKind::Decode,
        "plan_generated_column",
        "source schema does not match the supplied projection",
    )
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

pub(crate) fn unsafe_decimal_rescale(
    from: &arrow_schema::DataType,
    to: &arrow_schema::DataType,
) -> ArrowError {
    ArrowError::CastError(format!(
        "unsafe Decimal rescale from {from} to {to}: Arrow's i8 scale arithmetic would overflow"
    ))
}

pub(crate) fn invalid_cast_target(data_type: &arrow_schema::DataType) -> ArrowError {
    ArrowError::InvalidArgumentError(format!("invalid or unsupported cast target {data_type}"))
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
pub(crate) fn evolution_functor(name: &str, source: Error) -> Error {
    Error::new(
        source.kind(),
        "evolve_schema",
        format!("added column {name:?}: {source}"),
    )
    .with_source(source)
}
pub(crate) fn evolution_batch(source: ArrowError) -> Error {
    Error::new(ErrorKind::Decode, "evolve_schema", source.to_string()).with_source(source)
}
