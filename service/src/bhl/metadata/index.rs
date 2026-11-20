use super::{
    Table,
    error::{Result, invalid},
    tables,
};
use arrow_array::{Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use open_arrow_access_core::{Accessor, AccessorExt, ByteAccessor, ReadRequest};
use open_arrow_access_decompression::{DecompressionLayer, gzip::Gzip};
use open_arrow_access_file_format::csv::{CsvAccessor, CsvFormat};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::Path,
    sync::Arc,
};

#[derive(Clone, Serialize, Deserialize)]
pub struct SourceInfo {
    pub path: String,
    pub rows: u64,
    pub sha256: String,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct BhlMetadataInfo {
    pub version: u32,
    pub source_root: String,
    pub fingerprint: String,
    pub sources: BTreeMap<String, SourceInfo>,
}
#[derive(Clone)]
pub struct BhlMetadataRow {
    pub ordinal: u64,
    pub values: BTreeMap<String, String>,
}
fn hex(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
pub(super) fn digest(bytes: impl AsRef<[u8]>) -> String {
    hex(Sha256::digest(bytes))
}
fn get(db: &Connection, key: &str) -> Result<Option<String>> {
    Ok(db
        .query_row("SELECT value FROM meta WHERE key=?1", [key], |row| {
            row.get(0)
        })
        .optional()?)
}
fn set(db: &Connection, key: &str, value: &str) -> Result<()> {
    db.execute(
        "INSERT OR REPLACE INTO meta VALUES (?1,?2)",
        params![key, value],
    )?;
    Ok(())
}
pub(super) async fn prepare<A: ByteAccessor + 'static>(
    storage: A,
    root: &str,
    output: &Path,
) -> Result<BhlMetadataInfo> {
    if !output.is_absolute() || root.trim().is_empty() || output.exists() {
        return Err(invalid(
            "use a new absolute output path and a nonempty metadata source root",
        ));
    }
    fs::create_dir_all(output.parent().expect("absolute output"))?;
    let stage = output.with_extension("building.sqlite");
    let db = Connection::open(&stage)?;
    db.execute_batch("PRAGMA journal_mode=DELETE; CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL); CREATE TABLE IF NOT EXISTS sources(name TEXT PRIMARY KEY,info TEXT NOT NULL); CREATE TABLE IF NOT EXISTS rows(name TEXT NOT NULL,id TEXT NOT NULL,ordinal INTEGER NOT NULL,cells TEXT NOT NULL,PRIMARY KEY(name,ordinal)) WITHOUT ROWID; CREATE INDEX IF NOT EXISTS rows_key ON rows(name,id); CREATE UNIQUE INDEX IF NOT EXISTS single_key ON rows(name,id) WHERE name IN ('title','item','page','part');")?;
    let config = serde_json::to_string(&(1, root))?;
    if let Some(old) = get(&db, "config")? {
        if old != config {
            return Err(invalid(
                "metadata preparation source/version changed; use a new output",
            ));
        }
    } else {
        set(&db, "config", &config)?;
    }
    let input = Arc::new(storage).layer(DecompressionLayer::new(Gzip));
    let null_regex = regex::Regex::new(r"\b\B").expect("never matches: preserve empty cells");
    for table in tables() {
        let saved: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM sources WHERE name=?1)",
            [&table.name],
            |row| row.get(0),
        )?;
        if saved {
            continue;
        }
        // Arrow CSV decodes Utf8; final nested records use the fixed LargeUtf8 schema.
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
        db.execute_batch("BEGIN IMMEDIATE")?;
        let mut count = 0u64;
        let mut hash = Sha256::new();
        while let Some(batch) = reader.next().await? {
            let mut insert = db.prepare_cached("INSERT INTO rows VALUES (?1,?2,?3,?4)")?;
            for row in 0..batch.num_rows() {
                let cells = batch
                    .columns()
                    .iter()
                    .map(|column| {
                        let column = column
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| invalid("expected lexical metadata cells"))?;
                        if column.is_null(row) {
                            return Err(invalid("unexpected null metadata cell"));
                        }
                        Ok(column.value(row).to_owned())
                    })
                    .collect::<Result<Vec<_>>>()?;
                let id = super::super::pages::identity::number(&cells[table.key])?;
                let json = serde_json::to_string(&cells)?;
                hash.update(json.as_bytes());
                hash.update(b"\n");
                let ordinal =
                    i64::try_from(count).map_err(|_| invalid("too many metadata rows"))?;
                insert.execute(params![table.name, id.to_string(), ordinal, json])?;
                count += 1;
            }
        }
        let info = SourceInfo {
            path: format!("{}{path}", root.trim_end_matches('/').to_owned() + "/"),
            rows: count,
            sha256: hex(hash.finalize()),
        };
        db.execute(
            "INSERT INTO sources VALUES (?1,?2)",
            params![table.name, serde_json::to_string(&info)?],
        )?;
        db.execute_batch("COMMIT")?;
    }
    let mut sources = BTreeMap::new();
    {
        let mut statement = db.prepare("SELECT name,info FROM sources ORDER BY name")?;
        for row in statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })? {
            let (name, json) = row?;
            sources.insert(name, serde_json::from_str::<SourceInfo>(&json)?);
        }
    }
    let fingerprint = digest(serde_json::to_vec(&(1, root, &sources))?);
    let info = BhlMetadataInfo {
        version: 1,
        source_root: root.into(),
        fingerprint,
        sources,
    };
    set(&db, "complete", &serde_json::to_string(&info)?)?;
    db.close().map_err(|(_, error)| error)?;
    fs::hard_link(&stage, output)?;
    fs::remove_file(stage)?;
    Ok(info)
}
pub struct BhlMetadataIndex {
    db: Connection,
    pub info: BhlMetadataInfo,
    cache: HashMap<(String, u64), Arc<Vec<BhlMetadataRow>>>,
}
impl BhlMetadataIndex {
    pub fn open(path: &Path) -> open_arrow_access_core::Result<Self> {
        Self::open_inner(path).map_err(Into::into)
    }
    fn open_inner(path: &Path) -> Result<Self> {
        if !path.is_absolute() {
            return Err(invalid("metadata index must be an absolute local/NFS path"));
        }
        fs::metadata(path)?;
        let db = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        db.execute_batch("BEGIN")?;
        let info: BhlMetadataInfo = serde_json::from_str(
            &get(&db, "complete")?.ok_or_else(|| invalid("metadata snapshot is incomplete"))?,
        )?;
        let expected = tables();
        if info.version != 1
            || info.sources.len() != expected.len()
            || expected
                .iter()
                .any(|table| !info.sources.contains_key(&table.name))
            || info.fingerprint
                != digest(serde_json::to_vec(&(1, &info.source_root, &info.sources))?)
        {
            return Err(invalid(
                "invalid metadata snapshot version/sources/fingerprint",
            ));
        }
        Ok(Self {
            db,
            info,
            cache: HashMap::new(),
        })
    }
    pub fn rows(
        &mut self,
        name: &str,
        id: Option<u64>,
    ) -> open_arrow_access_core::Result<Vec<BhlMetadataRow>> {
        self.rows_inner(name, id).map_err(Into::into)
    }
    fn rows_inner(&mut self, name: &str, id: Option<u64>) -> Result<Vec<BhlMetadataRow>> {
        let Some(id) = id else {
            return Ok(vec![]);
        };
        let key = (name.to_owned(), id);
        if !self.cache.contains_key(&key) {
            let table: Table = tables()
                .into_iter()
                .find(|table| table.name == name)
                .ok_or_else(|| invalid(format!("unknown BHL metadata table: {name}")))?;
            let mut statement = self.db.prepare_cached(
                "SELECT ordinal,cells FROM rows WHERE name=?1 AND id=?2 ORDER BY ordinal",
            )?;
            let mut rows = Vec::new();
            for row in statement.query_map(params![name, id.to_string()], |row| {
                Ok((row.get::<_, u64>(0)?, row.get::<_, String>(1)?))
            })? {
                let (ordinal, json) = row?;
                let cells: Vec<String> = serde_json::from_str(&json)?;
                if cells.len() != table.fields.len() {
                    return Err(invalid("metadata row does not match fixed schema"));
                }
                rows.push(BhlMetadataRow {
                    ordinal,
                    values: table
                        .fields
                        .iter()
                        .map(|field| field.name().clone())
                        .zip(cells)
                        .collect(),
                });
            }
            self.cache.insert(key.clone(), Arc::new(rows));
        }
        Ok(self.cache[&key].as_ref().clone())
    }
}
