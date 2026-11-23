use std::collections::HashMap;

use open_arrow_access_core::{ObjectRef, Result};

use super::{error, join};
use crate::bhl::{BhlPageAlternative, BhlPageIdentity};

/// Reconcile identities only; never read or compare alternative OCR bodies.
pub(super) fn select(
    connection: &duckdb::Connection,
    keys: Vec<ObjectRef>,
    mut report: impl FnMut(&BhlPageAlternative) -> Result<()>,
) -> Result<Vec<ObjectRef>> {
    join::selection(connection, &keys)?;
    let mut statement = connection
        .prepare(
            "SELECT s.request_id, m.records, m.sequence
             FROM bhl_selection s
             LEFT JOIN LATERAL (
                 SELECT count(*) AS records, min(sequence) AS sequence FROM (
                     SELECT bhl_id(SequenceOrder) AS sequence FROM bhl_page
                     WHERE s.entity_type = 'item' AND __key = s.page_id
                     UNION ALL
                     SELECT bhl_id(SequenceOrder) FROM bhl_partpage
                     WHERE s.entity_type = 'part' AND __key = s.entity_id
                       AND bhl_id(PageID) = s.page_id
                 ) sequences
             ) m ON true ORDER BY s.request_id",
        )
        .map_err(error::duckdb)?;
    let mut rows = statement.query([]).map_err(error::duckdb)?;
    let mut groups = Vec::<(u64, Vec<usize>)>::new();
    let mut indices = HashMap::new();
    while let Some(row) = rows.next().map_err(error::duckdb)? {
        let index: usize = row.get(0).map_err(error::duckdb)?;
        let count: u64 = row.get(1).map_err(error::duckdb)?;
        let sequence: Option<u64> = row.get(2).map_err(error::duckdb)?;
        let object = &keys[index];
        let identity = BhlPageIdentity::parse(object.locator())?;
        if count != 1 || sequence.is_none() {
            return Err(crate::bhl::error::invalid_pages(format!(
                "{}: expected one authoritative SequenceOrder for {}:{} PageID {}; found {count} records, sequence {sequence:?}",
                object.locator(), identity.entity_type, identity.entity_id, identity.page_id,
            )).with_object(Some(object)));
        }
        let group = (identity.entity_type, identity.entity_id, identity.page_id);
        let next = groups.len();
        let slot = *indices.entry(group).or_insert(next);
        if slot == next {
            groups.push((sequence.unwrap(), Vec::new()));
        }
        groups[slot].1.push(index);
    }
    let mut selected = vec![false; keys.len()];
    for (sequence, candidates) in groups {
        let matching = candidates
            .iter()
            .copied()
            .filter(|&index| {
                BhlPageIdentity::parse(keys[index].locator())
                    .expect("validated identity")
                    .sequence
                    == sequence
            })
            .collect::<Vec<_>>();
        if matching.len() != 1 {
            let object = &keys[candidates[0]];
            return Err(crate::bhl::error::invalid_pages(format!(
                "expected exactly one OCR key matching metadata SequenceOrder={sequence}; found {}; candidates: {:?}",
                matching.len(), candidates.iter().map(|&i| keys[i].locator()).collect::<Vec<_>>()
            )).with_object(Some(object)));
        }
        let canonical = matching[0];
        selected[canonical] = true;
        for index in candidates {
            if index != canonical {
                report(&BhlPageAlternative {
                    canonical: keys[canonical].clone(),
                    alternative: keys[index].clone(),
                    sequence,
                })?;
            }
        }
    }
    Ok(keys
        .into_iter()
        .zip(selected)
        .filter_map(|(key, keep)| keep.then_some(key))
        .collect())
}
