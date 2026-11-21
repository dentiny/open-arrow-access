use super::{error, tables};
use arrow_array::{Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use duckdb::{Config, Connection, appender_params_from_iter, types::Value};
use futures::future::join_all;
use open_arrow_access_core::{Accessor, AccessorExt, ByteAccessor, ReadRequest, Result};
use open_arrow_access_decompression::{DecompressionLayer, gzip::Gzip};
use open_arrow_access_file_format::csv::{CsvAccessor, CsvFormat};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
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
    /// Canonical AWS dataset root URI (`s3://bhl-open-data/`).
    /// This identifies the upstream dataset even when loading from a mirror;
    /// it is not the caller's storage location or an immutable snapshot ID.
    pub source_root: String,
    pub fingerprint: String,
    /// Imported source tables keyed by table name (e.g. `title` or `page`).
    /// Each entry records the canonical AWS object URI, imported row count,
    /// and SHA-256 fingerprint of the decoded lexical cells.
    pub sources: BTreeMap<String, SourceInfo>,
}

/// All 13 AWS metadata tables in one in-memory DuckDB database.
pub struct BhlMetadata {
    pub info: BhlMetadataInfo,
    pub(super) connection: Mutex<Connection>,
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

async fn import_table(
    connection: Connection,
    input: Arc<dyn ByteAccessor>,
    table: super::Table,
) -> Result<(String, SourceInfo)> {
    let columns = table
        .fields
        .iter()
        .map(|field| format!("{} VARCHAR NOT NULL", quoted(field.name())))
        .collect::<Vec<_>>()
        .join(", ");
    connection
        .execute_batch(&format!(
            "CREATE TABLE {} (__key UBIGINT NOT NULL, __ordinal UBIGINT NOT NULL, {columns});",
            quoted(&table.name)
        ))
        .map_err(error::duckdb)?;
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
        .with_null_regex(regex::Regex::new(r"\b\B").expect("preserve empty cells"));
    let csv = CsvAccessor::new(input, schema).with_format(format);
    let path = format!("data/{}.txt.gz", table.name);
    let mut reader = csv.open(ReadRequest::new(path.clone())).await?;
    let mut count = 0u64;
    let mut hash = Sha256::new();
    let mut appender = connection.appender(&table.name).map_err(error::duckdb)?;
    while let Some(batch) = reader.next().await? {
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
            let values = [Value::UBigInt(key), Value::UBigInt(count)]
                .into_iter()
                .chain(cells.into_iter().map(|cell| Value::Text(cell.into())));
            appender
                .append_row(appender_params_from_iter(values))
                .map_err(error::duckdb)?;
            count += 1;
        }
    }
    // Appender drop suppresses write errors; flush explicitly before querying.
    appender.flush().map_err(error::duckdb)?;
    drop(appender);
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
    Ok((
        table.name,
        SourceInfo {
            path: format!("{}{path}", super::super::OPEN_DATA_BUCKET_URL),
            rows: count,
            sha256: hex(hash.finalize()),
        },
    ))
}

impl BhlMetadata {
    /// Import the 13 gzip TSV tables concurrently into an in-memory database.
    /// V1 assumes the source tables and query intermediates fit in memory.
    pub async fn load<A: ByteAccessor + 'static>(storage: A) -> Result<Self> {
        let config = Config::default()
            .with("enable_external_file_cache", "false")
            .map_err(error::duckdb)?;
        let connection = Connection::open_in_memory_with_flags(config).map_err(error::duckdb)?;
        connection
            .execute_batch(
                "CREATE MACRO bhl_id(value) AS CASE
             WHEN regexp_full_match(value, '[+]?[0-9]+')
             THEN nullif(try_cast(value AS UBIGINT), 0) END;",
            )
            .map_err(error::duckdb)?;
        let input: Arc<dyn ByteAccessor> =
            Arc::new(Arc::new(storage).layer(DecompressionLayer::new(Gzip)));
        let runtime = tokio::runtime::Handle::current();
        let mut imports = Vec::new();
        for table in tables() {
            let connection = connection.try_clone().map_err(error::duckdb)?;
            let input = input.clone();
            let runtime = runtime.clone();
            // One blocking worker/connection per table, with async source I/O.
            imports.push(tokio::task::spawn_blocking(move || {
                runtime.block_on(import_table(connection, input, table))
            }));
        }
        let mut sources = BTreeMap::new();
        // Join every importer before returning, including when one table fails.
        for imported in join_all(imports).await {
            let (name, info) = imported.map_err(error::task)??;
            sources.insert(name, info);
        }
        let source_root = super::super::OPEN_DATA_BUCKET_URL.to_owned();
        let fingerprint = hex(Sha256::digest(
            serde_json::to_vec(&(&source_root, &sources)).map_err(error::json)?,
        ));
        Ok(Self {
            connection: Mutex::new(connection),
            info: BhlMetadataInfo {
                source_root,
                fingerprint,
                sources,
            },
        })
    }
}
