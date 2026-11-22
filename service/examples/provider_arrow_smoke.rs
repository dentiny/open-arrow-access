//! Read real provider data to Arrow through EOF, without error recovery layers.
//!
//! cargo run -p open-arrow-access-service --features arxiv,bhl \
//!   --example provider_arrow_smoke -- arxiv /path/to/bulk/root src/shard.tar
//! cargo run -p open-arrow-access-service --features arxiv,bhl \
//!   --example provider_arrow_smoke -- bhl ocr/item-062861/
//!
//! BHL imports all 13 public metadata tables once in memory. An optional third
//! argument reuses an existing BhlMetadata database instead of downloading them.
use std::{
    collections::HashSet,
    error::Error,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow_array::{Array, LargeBinaryArray, LargeStringArray, ListArray, StructArray};
use open_arrow_access_core::{Accessor, ErrorKind, ListRequest, RecordReader, SchemaRef};
use open_arrow_access_service::{
    FileSetRequest,
    arxiv::{ArxivContentFormat, ArxivKind, ArxivReadRequest, ArxivService, content_schema},
    bhl::{
        BhlOpenData, BhlPageAccessor, BhlPageIdentity, metadata::BhlMetadata,
        open_data_bucket_operator, page_schema,
    },
    common::Connection,
};
use open_arrow_access_storage::{OpenDalAccessor, RetryConfig, StorageConfig, TimeoutConfig};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

fn network_config() -> StorageConfig {
    StorageConfig {
        retry: Some(RetryConfig::default()),
        timeout: Some(TimeoutConfig {
            io_timeout: Duration::from_secs(60),
            ..TimeoutConfig::default()
        }),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let started = Instant::now();
    match args.first().map(String::as_str) {
        Some("arxiv") if args.len() == 3 => {
            let operator = if args[1] == "aws" {
                opendal::Operator::new(
                    opendal::services::S3::default()
                        .bucket("arxiv")
                        .region("us-east-1")
                        .enable_request_payer(),
                )?
            } else {
                local_operator(&args[1])?
            };
            let records =
                ArxivService::new(OpenDalAccessor::new(network_config().apply(operator)?))
                    .with_metadata_connection(
                        Connection::new("https://oaipmh.arxiv.org")?
                            .with_http_config(network_config())?,
                    )
                    .records()
                    .content(ArxivContentFormat::Latex);
            let reader = records
                .open(ArxivReadRequest::new(ArxivKind::Source).with_shard(&args[2]))
                .await?;
            consume(reader, content_schema(), "article_id", None).await?;
        }
        Some("bhl") if matches!(args.len(), 2 | 3) => {
            read_bhl(&args[1], args.get(2).map(String::as_str)).await?;
        }
        Some("bhl-create") if matches!(args.len(), 2 | 3) => {
            create_metadata(&args[1], args.get(2).map(String::as_str)).await?;
        }
        Some("bhl-query") if args.len() == 3 => {
            query_metadata(&args[1], &args[2])?;
        }
        _ => {
            return Err(
                "usage: provider_arrow_smoke arxiv ROOT SHARD | bhl OCR_PREFIX [DB] | bhl-create DB [LOCAL_METADATA_ROOT] | bhl-query DB SQL"
                    .into(),
            );
        }
    }
    println!("elapsed_seconds={:.3}", started.elapsed().as_secs_f64());
    Ok(())
}

fn local_operator(root: &str) -> Result<opendal::Operator> {
    let root = std::fs::canonicalize(root)?;
    Ok(opendal::Operator::new(
        opendal::services::Fs::default().root(root.to_str().ok_or("non-UTF8 root")?),
    )?)
}

fn configure_query(metadata: &BhlMetadata) -> Result<()> {
    metadata
        .query_connection()?
        .execute_batch("SET memory_limit='4GB'; SET threads=2;")?;
    Ok(())
}

async fn read_bhl(prefix: &str, database: Option<&str>) -> Result<()> {
    let storage = OpenDalAccessor::new(network_config().apply(open_data_bucket_operator()?)?);
    let objects = BhlOpenData::new(storage.clone())
        .discover(ListRequest::new(prefix))
        .await?;
    let mut keys = Vec::new();
    let mut aggregates = 0;
    for object in objects {
        let key = object.locator();
        if !key.ends_with(".txt") {
            continue;
        }
        if let Some((directory, filename)) = key.rsplit_once('/')
            && let Some(group) = directory.rsplit('/').next()
            && (group.starts_with("item-") || group.starts_with("part-"))
            && filename.strip_suffix(".txt") == Some(group)
        {
            eprintln!("Excluded whole-item/part OCR text: {key}");
            aggregates += 1;
            continue;
        }
        BhlPageIdentity::parse(key)?;
        keys.push(object);
    }
    eprintln!("BHL excluded_aggregate_files={aggregates}");
    if keys.is_empty() {
        return Err("no OCR pages selected".into());
    }
    eprintln!("BHL selected {} OCR pages", keys.len());
    let metadata = if let Some(path) = database {
        eprintln!("Opening existing metadata database: {path}");
        BhlMetadata::open(path)?
    } else {
        eprintln!("Importing all 13 live AWS metadata tables");
        BhlMetadata::load(storage.clone()).await?
    };
    configure_query(&metadata)?;
    for (table, info) in &metadata.info.sources {
        eprintln!("metadata table={table} rows={}", info.rows);
    }
    let snapshot = metadata.info.fingerprint.clone();
    let pages = BhlPageAccessor::from_metadata(storage, snapshot, Arc::new(metadata), |page| {
        if !page.missing.is_empty() || !page.conflicts.is_empty() {
            return Err(open_arrow_access_core::Error::new(
                ErrorKind::Decode,
                "smoke_bhl_metadata",
                format!(
                    "{}: missing={:?}, conflicts={:?}",
                    page.object.locator(),
                    page.missing,
                    page.conflicts
                ),
            ));
        }
        Ok(())
    })?;
    let expected = keys
        .iter()
        .map(|key| format!("{OPEN_DATA_BUCKET_URL}{}", key.locator()))
        .collect();
    let reader = pages.open(FileSetRequest::new(keys)).await?;
    consume(reader, page_schema(), "source_path", Some(expected)).await?;
    Ok(())
}

async fn create_metadata(path: &str, mirror: Option<&str>) -> Result<()> {
    let operator = if let Some(root) = mirror {
        local_operator(root)?
    } else {
        open_data_bucket_operator()?
    };
    let storage = OpenDalAccessor::new(network_config().apply(operator)?);
    let config = duckdb::Config::default()
        .with("enable_external_file_cache", "false")?
        .with("memory_limit", "4GB")?
        .with("threads", "2")?;
    eprintln!("Importing all 13 metadata tables into {path}; mirror={mirror:?}");
    BhlMetadata::create_with_config(storage, path, config).await?;
    let metadata = BhlMetadata::open(path)?;
    for (table, info) in &metadata.info.sources {
        println!("metadata table={table} rows={}", info.rows);
    }
    println!(
        "PASS database_created=true fingerprint={}",
        metadata.info.fingerprint
    );
    Ok(())
}

fn query_metadata(path: &str, sql: &str) -> Result<()> {
    let metadata = BhlMetadata::open(path)?;
    configure_query(&metadata)?;
    let connection = metadata.query_connection()?;
    let mut statement = connection.prepare(sql)?;
    // Fetch with step() so later fetch errors remain fallible rather than panicking.
    let _ = statement.stream_arrow([])?;
    println!(
        "columns={:?}",
        statement
            .schema()
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>()
    );
    let mut rows = 0usize;
    let mut displayed = 0usize;
    while let Some(array) = statement.step()? {
        let batch = arrow_array::RecordBatch::from(&array);
        for column in batch.columns() {
            column.to_data().validate_full()?;
        }
        for row in 0..batch.num_rows().min(20 - displayed) {
            let values = batch
                .columns()
                .iter()
                .map(|column| arrow_cast::display::array_value_to_string(column.as_ref(), row))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            println!("{}", serde_json::to_string(&values)?);
            displayed += 1;
        }
        rows += batch.num_rows();
    }
    println!("PASS query_eof=true rows={rows} displayed_rows={displayed}");
    Ok(())
}

async fn consume(
    mut reader: Box<dyn RecordReader>,
    expected_schema: SchemaRef,
    identity_column: &str,
    expected_keys: Option<HashSet<String>>,
) -> Result<()> {
    let schema = reader.metadata().schema.clone();
    if schema.fields() != expected_schema.fields() {
        return Err("provider schema differs from the public Arrow schema".into());
    }
    println!(
        "columns={:?}",
        schema
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>()
    );
    let (mut rows, mut batches, mut payload_bytes, mut empty_payloads) =
        (0usize, 0usize, 0usize, 0usize);
    let mut identities = HashSet::new();
    let mut max_item_associations = 0;
    while let Some(batch) = reader.next().await? {
        if batch.schema() != schema {
            return Err("batch schema changed during the read".into());
        }
        for (field, column) in schema.fields().iter().zip(batch.columns()) {
            column.to_data().validate_full()?;
            if !field.is_nullable() && column.null_count() != 0 {
                return Err(format!("required column {} contains nulls", field.name()).into());
            }
        }
        let data = batch["data"]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or("data must be LargeBinary")?;
        let metadata = batch["metadata"]
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("metadata must be Struct")?;
        if metadata.null_count() != 0 {
            return Err("missing populated metadata".into());
        }
        let ids = batch[identity_column]
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .ok_or("identity must be LargeUtf8")?;
        for row in 0..batch.num_rows() {
            std::str::from_utf8(data.value(row))?;
            payload_bytes += data.value(row).len();
            empty_payloads += usize::from(data.value(row).is_empty());
            let inserted = identities.insert(ids.value(row).to_owned());
            if expected_keys.is_some() && !inserted {
                return Err(format!("duplicate output page: {}", ids.value(row)).into());
            }
            if let Some(item) = metadata.column_by_name("item") {
                let item = item
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or("metadata.item must retain all associations as a List")?;
                max_item_associations =
                    max_item_associations.max(usize::try_from(item.value_length(row))?);
            }
        }
        rows += batch.num_rows();
        batches += 1;
    }
    if rows == 0 {
        return Err("read reached EOF without any rows".into());
    }
    if let Some(expected) = expected_keys
        && (identities != expected || rows != expected.len())
    {
        return Err(format!(
            "selected {} pages but read {rows} rows / {} unique pages",
            expected.len(),
            identities.len()
        )
        .into());
    }
    println!(
        "PASS eof=true batches={batches} rows={rows} identities={} payload_bytes={payload_bytes} empty_payloads={empty_payloads} max_item_associations={max_item_associations}",
        identities.len()
    );
    Ok(())
}
