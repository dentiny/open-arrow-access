use super::{
    error::{decode, invalid},
    parser::PageProgress,
};
use chrono::NaiveDate;
use open_arrow_access_core::Result;

pub(super) fn validate_date_format(date: &NaiveDate, value: &str) -> Result<()> {
    if date.format("%Y-%m-%d").to_string() != value {
        return Err(invalid("dates must use YYYY-MM-DD"));
    }
    Ok(())
}

pub(super) fn validate_date_range(from: NaiveDate, until: NaiveDate) -> Result<()> {
    if from > until {
        return Err(invalid("date interval is reversed"));
    }
    Ok(())
}

pub(super) fn validate_page_status(status: &str, record_count: usize) -> Result<()> {
    match status {
        "no posts found" if record_count == 0 => Ok(()),
        "ok" => Ok(()),
        _ => Err(decode("bioRxiv API reported an error")),
    }
}

pub(super) fn validate_cursor(actual: u64, expected: u64) -> Result<()> {
    if actual != expected {
        return Err(decode("page cursor mismatch"));
    }
    Ok(())
}

pub(super) fn validate_page_progress(page: &PageProgress) -> Result<()> {
    if page.next > page.total || (page.next == page.cursor && page.cursor < page.total) {
        return Err(decode("inconsistent page counters"));
    }
    Ok(())
}
