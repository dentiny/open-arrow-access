use super::{error, tables};
use arrow_array::{Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use open_arrow_access_core::{Accessor, AccessorExt, ByteAccessor, ReadRequest, Result};
use open_arrow_access_decompression::{DecompressionLayer, gzip::Gzip};
use open_arrow_access_file_format::csv::{CsvAccessor, CsvFormat};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
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
#[derive(Clone)]
pub(super) struct Row {
    pub ordinal: u64,
    pub values: BTreeMap<String, String>,
}

/// AWS metadata held in memory for direct page/item/title/rights joins.
/// No database, persisted index or staging files are created.
pub struct BhlMetadata {
    pub info: BhlMetadataInfo,
    rows: HashMap<String, HashMap<u64, Vec<Row>>>,
}

fn hex(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

impl BhlMetadata {
    /// Stream all 13 `data/*.txt.gz` tables from bucket-root storage.
    /// Keep the returned value shared across selections/jobs to avoid refetching.
    /// Tables are decoded one batch at a time; retained source rows must fit memory.
    pub async fn load<A: ByteAccessor + 'static>(storage: A) -> Result<Self> {
        let input = Arc::new(storage).layer(DecompressionLayer::new(Gzip));
        let null_regex = regex::Regex::new(r"\b\B").expect("never matches: preserve empty cells");
        let mut rows = HashMap::new();
        let mut sources = BTreeMap::new();
        for table in tables() {
            // Arrow CSV decodes Utf8; final nested records use fixed LargeUtf8 fields.
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
            let mut selected = HashMap::<u64, Vec<Row>>::new();
            let mut count = 0u64;
            let mut hash = Sha256::new();
            while let Some(batch) = reader.next().await? {
                for row in 0..batch.num_rows() {
                    let cells = batch
                        .columns()
                        .iter()
                        .map(|column| {
                            let column = column
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .ok_or_else(|| error::invalid("expected lexical metadata cells"))?;
                            if column.is_null(row) {
                                return Err(error::invalid("unexpected null metadata cell"));
                            }
                            Ok(column.value(row).to_owned())
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let id = super::super::pages::identity::number(&cells[table.key])?;
                    let matches = selected.entry(id).or_default();
                    if table.single && !matches.is_empty() {
                        return Err(error::invalid(format!(
                            "duplicate numeric key in {}: {id}",
                            table.name
                        )));
                    }
                    hash.update(serde_json::to_vec(&cells).map_err(error::json)?);
                    hash.update(b"\n");
                    matches.push(Row {
                        ordinal: count,
                        values: table
                            .fields
                            .iter()
                            .map(|field| field.name().clone())
                            .zip(cells)
                            .collect(),
                    });
                    count = count
                        .checked_add(1)
                        .ok_or_else(|| error::invalid("too many metadata rows"))?;
                }
            }
            sources.insert(
                table.name.clone(),
                SourceInfo {
                    path: format!("{}{path}", super::super::OPEN_DATA_BUCKET_URL),
                    rows: count,
                    sha256: hex(hash.finalize()),
                },
            );
            rows.insert(table.name, selected);
        }
        let source_root = super::super::OPEN_DATA_BUCKET_URL.to_owned();
        let fingerprint = hex(Sha256::digest(
            serde_json::to_vec(&(&source_root, &sources)).map_err(error::json)?,
        ));
        Ok(Self {
            rows,
            info: BhlMetadataInfo {
                source_root,
                fingerprint,
                sources,
            },
        })
    }

    pub(super) fn rows(&self, name: &str, id: Option<u64>) -> Result<Vec<Row>> {
        let table = self
            .rows
            .get(name)
            .ok_or_else(|| error::invalid(format!("unknown BHL metadata table: {name}")))?;
        Ok(id
            .and_then(|id| table.get(&id))
            .cloned()
            .unwrap_or_default())
    }
}
