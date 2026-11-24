//! CourtListener bulk object path parsing.
use super::validation::validate_snapshot;

pub(super) struct SnapshotObject<'a> {
    pub table: &'a str,
    pub date: &'a str,
}

pub(super) fn parse_object(locator: &str) -> Option<SnapshotObject<'_>> {
    let stem = locator
        .strip_prefix("bulk-data/")?
        .strip_suffix(".csv.bz2")?;
    let (table, date) = stem.split_at_checked(stem.len().checked_sub(11)?)?;
    let date = date.strip_prefix('-')?;
    validate_snapshot(table, date).ok()?;
    Some(SnapshotObject { table, date })
}
