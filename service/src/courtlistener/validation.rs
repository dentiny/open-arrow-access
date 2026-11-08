use open_arrow_access_core::Result;

use super::error::invalid;

pub(super) fn validate_snapshot(table: &str, date: &str) -> Result<()> {
    validate_table(table)?;
    if !is_valid_date(date) {
        return Err(invalid());
    }
    Ok(())
}

pub(super) fn validate_table(table: &str) -> Result<()> {
    if !table.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        || !table.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
    {
        return Err(invalid());
    }
    Ok(())
}

pub(super) fn parse_object(locator: &str) -> Option<(&str, &str)> {
    let stem = locator
        .strip_prefix("bulk-data/")?
        .strip_suffix(".csv.bz2")?;
    let (table, date) = stem.split_at_checked(stem.len().checked_sub(11)?)?;
    let date = date.strip_prefix('-')?;
    validate_snapshot(table, date).ok()?;
    Some((table, date))
}

fn is_valid_date(date: &str) -> bool {
    let bytes = date.as_bytes();
    if bytes.len() != 10
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || !bytes
            .iter()
            .enumerate()
            .all(|(i, b)| i == 4 || i == 7 || b.is_ascii_digit())
    {
        return false;
    }
    let year: u32 = date[..4].parse().unwrap();
    let month: u32 = date[5..7].parse().unwrap();
    let day: u32 = date[8..].parse().unwrap();
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let days = match month {
        2 => 28 + u32::from(leap),
        4 | 6 | 9 | 11 => 30,
        1..=12 => 31,
        _ => 0,
    };
    year > 0 && day > 0 && day <= days
}
