use super::{
    BhlPageIdentity,
    identity::{number, optional_number},
    schema::{Row, profile},
};
use open_arrow_access_core::{RecordBatchRow, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Sidecar {
    version: u32,
    snapshot_id: String,
    keys: Vec<String>,
    source_tables: BTreeMap<String, serde_json::Value>,
    tables: BTreeMap<String, Vec<Row>>,
}

/// Per-page reconciliation. Missing joins never remove a page; conflicts retain
/// both original source rows and use filename identity/order at the top level.
#[derive(Clone, Debug, Default, Serialize)]
pub struct BhlJoinReport {
    pub missing: Vec<String>,
    pub conflicts: Vec<String>,
}

struct Table {
    rows: Vec<Row>,
    index: HashMap<u64, Vec<usize>>,
}

/// Validated, shard-scoped source tables. Whole-corpus TSVs belong in a disk index,
/// not in every worker's memory. All 13 acquired tables must be represented.
pub struct BhlMetadata {
    pub snapshot_id: String,
    keys: HashSet<String>,
    tables: BTreeMap<String, Table>,
    pub source_tables: BTreeMap<String, serde_json::Value>,
}

pub(super) struct JoinedPage<'a> {
    pub identity: BhlPageIdentity,
    pub tables: BTreeMap<String, Vec<&'a Row>>,
    pub report: BhlJoinReport,
}

impl BhlMetadata {
    /// Parse an offline-prepared sidecar; reject missing tables, unknown fields,
    /// duplicate single-row IDs, invalid join keys or duplicate selected keys.
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        let sidecar: Sidecar = serde_json::from_slice(bytes)
            .map_err(|e| super::invalid(format!("invalid BHL metadata sidecar: {e}")))?;
        if sidecar.version != 1 || sidecar.snapshot_id.trim().is_empty() || sidecar.keys.is_empty()
        {
            return Err(super::invalid(
                "invalid BHL sidecar version/snapshot/selection",
            ));
        }
        let mut keys = HashSet::new();
        for key in sidecar.keys {
            BhlPageIdentity::parse(&key)?;
            if !keys.insert(key) {
                return Err(super::invalid("duplicate BHL selected key"));
            }
        }
        let expected = profile()
            .iter()
            .map(|p| p.name.as_str())
            .collect::<BTreeSet<_>>();
        if sidecar
            .tables
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
            != expected
            || sidecar
                .source_tables
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>()
                != expected
        {
            return Err(super::invalid(
                "all 13 acquired BHL source tables are required",
            ));
        }
        let mut tables = BTreeMap::new();
        for p in profile() {
            let provenance = &sidecar.source_tables[&p.name];
            let source_key = format!("data/{}.txt.gz", p.name);
            let valid_hash = provenance
                .get("sha256")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|hash| {
                    hash.len() == 64 && hash.bytes().all(|c| c.is_ascii_hexdigit())
                });
            if provenance.get("key").and_then(serde_json::Value::as_str)
                != Some(source_key.as_str())
                || !valid_hash
                || !provenance
                    .get("rows")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|rows| rows >= sidecar.tables[&p.name].len() as u64)
            {
                return Err(super::invalid(format!(
                    "invalid BHL {} acquisition provenance",
                    p.name
                )));
            }
            let rows = sidecar.tables[&p.name].clone();
            let mut index: HashMap<u64, Vec<usize>> = HashMap::new();
            for (position, row) in rows.iter().enumerate() {
                if row.keys().map(String::as_str).collect::<BTreeSet<_>>()
                    != p.columns
                        .iter()
                        .map(String::as_str)
                        .collect::<BTreeSet<_>>()
                {
                    return Err(super::invalid(format!("unexpected BHL {} columns", p.name)));
                }
                let id = number(&row[&p.key])?;
                let matches = index.entry(id).or_default();
                if p.single && !matches.is_empty() {
                    return Err(super::invalid(format!("duplicate BHL {} ID: {id}", p.name)));
                }
                matches.push(position);
            }
            tables.insert(p.name.clone(), Table { rows, index });
        }
        Ok(Self {
            snapshot_id: sidecar.snapshot_id,
            keys,
            tables,
            source_tables: sidecar.source_tables,
        })
    }

    fn rows(&self, table: &str, id: Option<u64>) -> Vec<&Row> {
        let table = &self.tables[table];
        id.and_then(|id| table.index.get(&id))
            .into_iter()
            .flatten()
            .map(|&i| &table.rows[i])
            .collect()
    }

    pub fn report(&self, key: &str) -> Result<BhlJoinReport> {
        Ok(self.join(key)?.report)
    }

    pub(super) fn join(&self, key: &str) -> Result<JoinedPage<'_>> {
        if !self.keys.contains(key) {
            return Err(super::invalid("OCR key is outside BHL metadata selection"));
        }
        let identity = BhlPageIdentity::parse(key)?;
        let mut selected = BTreeMap::new();
        let mut report = BhlJoinReport::default();
        let part_id = (identity.entity_type == "part").then_some(identity.entity_id);
        let part = self.rows("part", part_id);
        let page = self.rows("page", Some(identity.page_id));
        let partpage = self
            .rows("partpage", part_id)
            .into_iter()
            .filter_map(|row| match number(&row["PageID"]) {
                Ok(id) if id == identity.page_id => Some(Ok(row)),
                Ok(_) => None,
                Err(e) => Some(Err(e)),
            })
            .collect::<Result<Vec<_>>>()?;
        let mut parents = Vec::new();
        if part_id.is_none() {
            parents.push(identity.entity_id);
        }
        for row in part.iter().chain(partpage.iter()).chain(page.iter()) {
            if let Some(id) = optional_number(&row["ItemID"])?
                && !parents.contains(&id)
            {
                parents.push(id);
            }
        }
        if parents.len() > 1 {
            report.conflicts.push("item-parent".into());
        }
        let item_id = parents.first().copied();
        let item = self.rows("item", item_id);
        let title_id = item
            .first()
            .map(|row| optional_number(&row["TitleID"]))
            .transpose()?
            .flatten();
        let title = self.rows("title", title_id);
        for (name, rows, required) in [
            ("page", &page, true),
            ("item", &item, true),
            ("title", &title, true),
            ("part", &part, part_id.is_some()),
            ("partpage", &partpage, part_id.is_some()),
        ] {
            if required && rows.is_empty() {
                report.missing.push(name.into());
            }
        }
        for row in if part_id.is_some() { &partpage } else { &page } {
            if number(&row["SequenceOrder"])? != identity.sequence {
                report.conflicts.push("page-sequence".into());
            }
        }
        let creators = self.rows("creator", title_id);
        let partcreators = self.rows("partcreator", part_id);
        let creator_ids = creators
            .iter()
            .chain(partcreators.iter())
            .map(|row| optional_number(&row["CreatorID"]))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<HashSet<_>>();
        // Iterate table source order, never creator-ID order; retain source duplicates.
        let creatoridentifier = self.tables["creatoridentifier"]
            .rows
            .iter()
            .filter_map(|row| match number(&row["CreatorID"]) {
                Ok(id) if creator_ids.contains(&id) => Some(Ok(row)),
                Ok(_) => None,
                Err(e) => Some(Err(e)),
            })
            .collect::<Result<Vec<_>>>()?;
        let doi = self.tables["doi"]
            .rows
            .iter()
            .filter_map(|row| {
                let expected = match row["EntityType"].to_ascii_lowercase().as_str() {
                    "title" => title_id,
                    "item" => item_id,
                    "page" => Some(identity.page_id),
                    "part" => part_id,
                    _ => None,
                };
                match number(&row["EntityID"]) {
                    Ok(id) if Some(id) == expected => Some(Ok(row)),
                    Ok(_) => None,
                    Err(e) => Some(Err(e)),
                }
            })
            .collect::<Result<Vec<_>>>()?;
        selected.insert("title".into(), title);
        selected.insert("item".into(), item);
        selected.insert("page".into(), page);
        selected.insert("part".into(), part);
        selected.insert("partpage".into(), partpage);
        selected.insert("creator".into(), creators);
        selected.insert("partcreator".into(), partcreators);
        selected.insert("creatoridentifier".into(), creatoridentifier);
        selected.insert("doi".into(), doi);
        for name in ["subject", "titleidentifier"] {
            selected.insert(name.into(), self.rows(name, title_id));
        }
        selected.insert(
            "partidentifier".into(),
            self.rows("partidentifier", part_id),
        );
        selected.insert(
            "pagename".into(),
            self.rows("pagename", Some(identity.page_id)),
        );
        report.conflicts.sort();
        report.conflicts.dedup();
        Ok(JoinedPage {
            identity,
            tables: selected,
            report,
        })
    }
}

impl JoinedPage<'_> {
    pub(super) fn append(&self, row: &mut RecordBatchRow<'_>) -> Result<()> {
        row.append_struct("metadata", |metadata| {
            for p in profile() {
                let rows = &self.tables[&p.name];
                let write = |output: &mut RecordBatchRow<'_>, source: &&Row| {
                    for name in &p.columns {
                        output.append(name, source[name].as_str())?;
                    }
                    Ok(())
                };
                if p.single {
                    if let Some(source) = rows.first() {
                        metadata.append_struct(&p.name, |output| write(output, source))?;
                    }
                } else {
                    metadata.append_structs(&p.name, rows, write)?;
                }
            }
            Ok(())
        })
    }
}
