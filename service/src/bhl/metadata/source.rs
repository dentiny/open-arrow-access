use super::{error, tables};
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use duckdb::{Config, Connection, appender_params_from_iter, types::Value};
use open_arrow_access_core::{Accessor, AccessorExt, ByteAccessor, ReadRequest, Result};
use open_arrow_access_decompression::{DecompressionLayer, gzip::Gzip};
use open_arrow_access_file_format::csv::{CsvAccessor, CsvFormat};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

#[derive(Clone, Serialize)]
pub struct SourceInfo {
    pub path: String,
    pub rows: u64,
    /// Hash of decoded lexical cell rows, not compressed bytes or AWS versions.
    pub sha256: String,
}
#[derive(Clone, Serialize)]
pub struct BhlMetadataInfo {
    pub source_root: String,
    pub fingerprint: String,
    pub sources: BTreeMap<String, SourceInfo>,
}

/// Resource settings for the ephemeral DuckDB database, not a publication index.
#[derive(Clone, Debug)]
pub struct BhlMetadataOptions {
    /// DuckDB buffer-manager limit; this is not a bound on total process memory.
    pub memory_limit: String,
    /// Parent of a private temporary directory containing the database and spill files.
    pub temporary_directory: Option<PathBuf>,
}
impl Default for BhlMetadataOptions {
    fn default() -> Self {
        Self {
            memory_limit: "1GB".into(),
            temporary_directory: None,
        }
    }
}

/// AWS source rows held in an ephemeral, disk-backed DuckDB database.
/// Load once and share across selections; selection queries are serialized.
pub struct BhlMetadata {
    pub info: BhlMetadataInfo,
    pub(super) connection: Mutex<Connection>,
    // Drop the connection before deleting its database and spill directory.
    _directory: tempfile::TempDir,
}

fn hex(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

// Fixed schema identifiers only; source cells always go through bound values.
fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn append_batch(
    connection: &Connection,
    table: &super::Table,
    batch: &RecordBatch,
    count: &mut u64,
    hash: &mut Sha256,
) -> Result<()> {
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| error::invalid("expected lexical metadata cells"))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut appender = connection.appender(&table.name).map_err(error::duckdb)?;
    for row in 0..batch.num_rows() {
        let cells = columns
            .iter()
            .map(|column| {
                if column.is_null(row) {
                    return Err(error::invalid("unexpected null metadata cell"));
                }
                Ok(column.value(row))
            })
            .collect::<Result<Vec<_>>>()?;
        let key = super::super::pages::identity::number(cells[table.key])?;
        hash.update(serde_json::to_vec(&cells).map_err(error::json)?);
        hash.update(b"\n");
        let values = [Value::UBigInt(key), Value::UBigInt(*count)]
            .into_iter()
            .chain(cells.into_iter().map(|cell| Value::Text(cell.into())));
        appender
            .append_row(appender_params_from_iter(values))
            .map_err(error::duckdb)?;
        *count = count
            .checked_add(1)
            .ok_or_else(|| error::invalid("too many metadata rows"))?;
    }
    // DuckDB can defer constraint/write errors until flush. Never rely on Drop.
    appender.flush().map_err(error::duckdb)
}

fn finish_table(connection: &Connection, table: &super::Table) -> Result<()> {
    connection.execute_batch("COMMIT;").map_err(error::duckdb)?;
    if table.single {
        let duplicate = connection.query_row(
            &format!(
                "SELECT __key FROM {} GROUP BY __key HAVING count(*) > 1 LIMIT 1",
                quoted(&table.name)
            ),
            [],
            |row| row.get::<_, u64>(0),
        );
        match duplicate {
            Ok(key) => {
                return Err(error::invalid(format!(
                    "duplicate numeric key in {}: {key}",
                    table.name
                )));
            }
            Err(duckdb::Error::QueryReturnedNoRows) => {}
            Err(source) => return Err(error::duckdb(source)),
        }
    }
    let fields = table
        .fields
        .iter()
        .map(|field| {
            let name = quoted(field.name());
            format!("{name} := {name}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    connection
        .execute_batch(&format!(
            "CREATE VIEW bhl_{} AS SELECT *, struct_pack({fields}) AS __record FROM {};",
            table.name,
            quoted(&table.name)
        ))
        .map_err(error::duckdb)?;
    Ok(())
}

impl BhlMetadata {
    /// Stream and validate all 13 AWS gzip TSV tables into temporary DuckDB storage.
    pub async fn load<A: ByteAccessor + 'static>(storage: A) -> Result<Self> {
        Self::load_with_options(storage, BhlMetadataOptions::default()).await
    }

    pub async fn load_with_options<A: ByteAccessor + 'static>(
        storage: A,
        options: BhlMetadataOptions,
    ) -> Result<Self> {
        let directory = match &options.temporary_directory {
            Some(parent) => tempfile::Builder::new()
                .prefix("bhl-duckdb-")
                .tempdir_in(parent),
            None => tempfile::Builder::new().prefix("bhl-duckdb-").tempdir(),
        }
        .map_err(error::io)?;
        // A running blocking insert retains the directory even if load is cancelled.
        let directory = Arc::new(directory);
        let spill = directory.path().join("spill");
        let config = Config::default()
            .with("memory_limit", &options.memory_limit)
            .map_err(error::duckdb)?
            .with(
                "temp_directory",
                spill
                    .to_str()
                    .ok_or_else(|| error::invalid("non-UTF8 temporary path"))?,
            )
            .map_err(error::duckdb)?;
        let mut connection =
            Connection::open_with_flags(directory.path().join("metadata.duckdb"), config)
                .map_err(error::duckdb)?;
        connection
            .execute_batch(
                "CREATE MACRO bhl_id(value) AS CASE
               WHEN regexp_full_match(value, '[+]?[0-9]+')
               THEN nullif(try_cast(value AS UBIGINT), 0) END;
             CREATE TEMP TABLE bhl_selection (
               request_id UBIGINT, entity_type VARCHAR, entity_id UBIGINT,
               page_id UBIGINT, sequence UBIGINT);",
            )
            .map_err(error::duckdb)?;
        let input = Arc::new(storage).layer(DecompressionLayer::new(Gzip));
        let null_regex = regex::Regex::new(r"\b\B").expect("never matches: preserve empty cells");
        let mut sources = BTreeMap::new();
        for table in tables() {
            let columns = table
                .fields
                .iter()
                .map(|field| format!("{} VARCHAR NOT NULL", quoted(field.name())))
                .collect::<Vec<_>>()
                .join(", ");
            connection.execute_batch(&format!(
                "CREATE TABLE {} (__key UBIGINT NOT NULL, __ordinal UBIGINT NOT NULL, {columns}); BEGIN;",
                quoted(&table.name)
            )).map_err(error::duckdb)?;
            let schema = Arc::new(Schema::new(
                table
                    .fields
                    .iter()
                    .map(|field| Field::new(field.name(), DataType::Utf8, false))
                    .collect::<Vec<_>>(),
            ));
            let format = CsvFormat::default()
                .with_header(true)
                .with_header_validation(true)
                .with_delimiter(b'\t')
                .with_quote(0)
                .with_null_regex(null_regex.clone());
            let csv = CsvAccessor::new(input.clone(), schema).with_format(format);
            let path = format!("data/{}.txt.gz", table.name);
            let mut reader = csv.open(ReadRequest::new(path.clone())).await?;
            let mut count = 0u64;
            let mut hash = Sha256::new();
            while let Some(batch) = reader.next().await? {
                let table = table.clone();
                let keep_directory = directory.clone();
                // DuckDB insertion is blocking; do not run it on the async I/O worker.
                (connection, count, hash) = tokio::task::spawn_blocking(move || {
                    let _keep_directory = keep_directory;
                    append_batch(&connection, &table, &batch, &mut count, &mut hash)?;
                    Ok::<_, open_arrow_access_core::Error>((connection, count, hash))
                })
                .await
                .map_err(error::task)??;
            }
            let loaded_table = table.clone();
            let keep_directory = directory.clone();
            connection = tokio::task::spawn_blocking(move || {
                let _keep_directory = keep_directory;
                finish_table(&connection, &loaded_table)?;
                Ok::<_, open_arrow_access_core::Error>(connection)
            })
            .await
            .map_err(error::task)??;
            sources.insert(
                table.name,
                SourceInfo {
                    path: format!("{}{path}", super::super::OPEN_DATA_BUCKET_URL),
                    rows: count,
                    sha256: hex(hash.finalize()),
                },
            );
        }
        let keep_directory = directory.clone();
        connection = tokio::task::spawn_blocking(move || {
            let _keep_directory = keep_directory;
            connection
                .execute_batch("CHECKPOINT;")
                .map_err(error::duckdb)?;
            Ok::<_, open_arrow_access_core::Error>(connection)
        })
        .await
        .map_err(error::task)??;
        let source_root = super::super::OPEN_DATA_BUCKET_URL.to_owned();
        let fingerprint = hex(Sha256::digest(
            serde_json::to_vec(&(&source_root, &sources)).map_err(error::json)?,
        ));
        let directory = Arc::try_unwrap(directory)
            .map_err(|_| error::invalid("metadata import still running"))?;
        Ok(Self {
            connection: Mutex::new(connection),
            _directory: directory,
            info: BhlMetadataInfo {
                source_root,
                fingerprint,
                sources,
            },
        })
    }
}
