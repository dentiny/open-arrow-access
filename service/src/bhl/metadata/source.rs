use super::{error, tables};
use crate::duckdb::ManagedConnection;
use arrow_array::cast::AsArray;
use arrow_schema::{DataType, Field, Schema};
use futures::future::join_all;
use open_arrow_access_core::{Accessor, AccessorExt, ByteAccessor, ReadRequest, Result};
use open_arrow_access_decompression::{DecompressionLayer, gzip::Gzip};
use open_arrow_access_file_format::csv::{CsvAccessor, CsvFormat};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[derive(Clone, Serialize, Deserialize)]
pub struct SourceInfo {
    pub path: String,
    pub rows: u64,
    /// Hash of decoded lexical cell rows, not compressed bytes or AWS versions.
    pub sha256: String,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct BhlMetadataInfo {
    /// Canonical AWS dataset root URI (`s3://bhl-open-data/`).
    /// This identifies the upstream dataset even when loading from a mirror;
    /// it is not the caller's storage location or an immutable snapshot ID.
    pub source_root: String,
    pub fingerprint: String,
    /// Imported TSV tables keyed by name, with source URI, row count and checksum.
    /// Empty for externally opened databases, which use the caller's fingerprint.
    pub sources: BTreeMap<String, SourceInfo>,
}

/// All 13 AWS metadata tables in one in-memory or persistent DuckDB database.
pub struct BhlMetadata {
    pub info: BhlMetadataInfo,
    pub(super) connection: Mutex<ManagedConnection>,
    pub(super) query_sql: &'static str,
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
    connection: ManagedConnection,
    input: Arc<dyn ByteAccessor>,
    table: super::Table,
) -> Result<(String, SourceInfo)> {
    let columns = table
        .fields
        .iter()
        .map(|field| format!("{} VARCHAR NOT NULL", quoted(field.name())))
        .collect::<Vec<_>>()
        .join(", ");
    let schema = Arc::new(Schema::new(
        table
            .fields
            .iter()
            .map(|field| Field::new(field.name(), DataType::Utf8, false))
            .collect::<Vec<_>>(),
    ));
    // Anchor the empty intersection so failed null matches never scan cell contents.
    let format = CsvFormat::default()
        .with_header(true)
        .with_header_validation(true)
        .with_delimiter(b'\t')
        .with_quote(0)
        .with_null_regex(regex::Regex::new(r"\A[a&&b]").expect("preserve empty cells"));
    let csv = CsvAccessor::new(input, schema).with_format(format);
    let path = format!("data/{}.txt.gz", table.name);
    let mut reader = csv.open(ReadRequest::new(path.clone())).await?;
    let import_name = table.name.clone();
    let key_column = table.key;
    let source_path = format!("{}{path}", super::super::OPEN_DATA_BUCKET_URL);
    tokio::task::spawn_blocking(move || {
        connection
            .execute_batch(&format!(
                "CREATE TABLE {} ({columns});",
                quoted(&import_name)
            ))
            .map_err(error::duckdb)?;
        let mut count = 0u64;
        let mut hash = Sha256::new();
        // Avoid repeated auto-commits while a persistent table is imported.
        connection
            .execute_batch("BEGIN TRANSACTION")
            .map_err(error::duckdb)?;
        let mut appender = connection.appender(&import_name).map_err(error::duckdb)?;
        // Keep one appender for the table. Network reads remain asynchronous on
        // the runtime while this blocking importer owns DuckDB's native state.
        while let Some(batch) = futures::executor::block_on(reader.next())? {
            // CsvAccessor decoded the fixed non-nullable Utf8 schema above;
            // Arrow has already checked the column types and nullability.
            let columns = batch
                .columns()
                .iter()
                .map(|column| column.as_string::<i32>())
                .collect::<Vec<_>>();
            for row in 0..batch.num_rows() {
                let cells = columns
                    .iter()
                    .map(|column| column.value(row))
                    .collect::<Vec<_>>();
                super::validation::validate_source_key(cells[key_column])?;
                // Preserve the decoded lexical-row fingerprint exactly.
                hash.update(serde_json::to_vec(&cells).map_err(error::json)?);
                hash.update(b"\n");
                count += 1;
            }
            appender.append_record_batch(batch).map_err(error::duckdb)?;
        }
        // Appender::drop suppresses write errors; flush once, explicitly.
        appender.flush().map_err(error::duckdb)?;
        drop(appender);
        connection.execute_batch("COMMIT").map_err(error::duckdb)?;
        connection.close()?;
        Ok((
            import_name,
            SourceInfo {
                path: source_path,
                rows: count,
                sha256: hex(hash.finalize()),
            },
        ))
    })
    .await
    .map_err(error::task)?
}

impl BhlMetadata {
    /// Import the 13 gzip TSV tables concurrently into an in-memory database.
    /// V1 assumes the source tables and query intermediates fit in memory.
    pub async fn load<A: ByteAccessor + 'static>(storage: A) -> Result<Self> {
        let connection = ManagedConnection::in_memory()?;
        Self::import(storage, connection).await
    }

    pub(super) async fn import<A: ByteAccessor + 'static>(
        storage: A,
        connection: ManagedConnection,
    ) -> Result<Self> {
        let input: Arc<dyn ByteAccessor> =
            Arc::new(Arc::new(storage).layer(DecompressionLayer::new(Gzip)));
        let mut imports = Vec::new();
        for table in tables() {
            let connection = connection.try_clone()?;
            let input = input.clone();
            imports.push(import_table(connection, input, table));
        }
        let mut sources = BTreeMap::new();
        // Join every importer before returning, including when one table fails.
        for imported in join_all(imports).await {
            let (name, info) = imported?;
            sources.insert(name, info);
        }
        let source_root = super::super::OPEN_DATA_BUCKET_URL.to_owned();
        let fingerprint = hex(Sha256::digest(
            serde_json::to_vec(&(&source_root, &sources)).map_err(error::json)?,
        ));
        let info = BhlMetadataInfo {
            source_root,
            fingerprint,
            sources,
        };
        tokio::task::spawn_blocking(move || Self::from_connection(connection, info))
            .await
            .map_err(error::task)?
    }
}
