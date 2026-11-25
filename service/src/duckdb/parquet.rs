use super::{ManagedConnection, error};
use open_arrow_access_core::{Error, ErrorKind, Result};

/// Options for external Parquet views. Defaults require compatible file schemas
/// and expose only columns stored in the files, without inferred partition columns.
#[derive(Clone, Copy, Debug, Default)]
pub struct ParquetOptions {
    /// Match columns by name across files, filling unavailable columns with NULL.
    pub union_by_name: bool,
    /// Read `key=value` directory components as additional partition columns.
    pub hive_partitioning: bool,
}

pub(super) fn register(
    connection: &ManagedConnection,
    table: &str,
    paths: &[impl AsRef<str>],
    options: ParquetOptions,
) -> Result<()> {
    let invalid = |message| Error::new(ErrorKind::InvalidInput, "duckdb_parquet", message);
    if table.is_empty() || table.contains('\0') {
        return Err(invalid(
            "Parquet table name must be nonempty and contain no NUL",
        ));
    }
    if paths.is_empty() {
        return Err(invalid(
            "Parquet registration requires at least one file or glob",
        ));
    }
    let mut remote = false;
    let mut literals = Vec::with_capacity(paths.len());
    for path in paths {
        let path = path.as_ref();
        if path.is_empty() || path.contains('\0') {
            return Err(invalid("Parquet paths must be nonempty and contain no NUL"));
        }
        if let Ok(url) = url::Url::parse(path)
            && path.contains("://")
            && !std::path::Path::new(path).is_absolute()
        {
            match url.scheme() {
                "http" | "https" | "s3" => remote = true,
                "file" => (),
                _ => {
                    return Err(Error::new(
                        ErrorKind::Unsupported,
                        "duckdb_parquet",
                        "remote Parquet URLs must use HTTP(S) or S3 through cache_httpfs",
                    ));
                }
            }
        }
        literals.push(format!("'{}'", path.replace('\'', "''")));
    }
    if remote {
        connection.configure_parquet_cache()?;
    }
    // CREATE VIEW does not support bound file parameters. Quote identifiers and
    // string literals separately, keeping caller paths out of SQL syntax.
    let table = table.replace('"', "\"\"");
    connection
        .execute_batch(&format!(
            "CREATE VIEW main.\"{table}\" AS SELECT * FROM read_parquet([{}],
             union_by_name = {}, hive_partitioning = {});",
            literals.join(","),
            options.union_by_name,
            options.hive_partitioning
        ))
        .map_err(error::database)
}

