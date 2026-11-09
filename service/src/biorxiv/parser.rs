use super::{
    error::{self, decode},
    validation,
};
use crate::common::{array, text};
use chrono::NaiveDate;
use open_arrow_access_core::Result;
use serde_json::Value;

pub(super) struct PageProgress {
    pub(super) cursor: u64,
    pub(super) next: u64,
    pub(super) total: u64,
}

pub(super) fn parse_date(value: &str) -> Result<NaiveDate> {
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(error::date)?;
    validation::validate_date_format(&date, value)?;
    Ok(date)
}

fn parse_integer(value: &Value, key: &str) -> Result<u64> {
    let value = value
        .get(key)
        .ok_or_else(|| decode("missing page counter"))?;
    value
        .as_u64()
        .or_else(|| value.as_str()?.parse().ok())
        .ok_or_else(|| decode("invalid page counter"))
}

pub(super) fn parse_next_cursor(value: &Value, cursor: u64) -> Result<Option<u64>> {
    let records = array(value, "collection")?;
    let message = array(value, "messages")?
        .first()
        .ok_or_else(|| decode("missing page status"))?;
    let status = text(message, "status")?;
    validation::validate_page_status(status, records.len())?;
    if status == "no posts found" {
        return Ok(None);
    }
    validation::validate_cursor(parse_integer(message, "cursor")?, cursor)?;
    let total = parse_integer(message, "total")?;
    let next = cursor
        .checked_add(records.len() as u64)
        .ok_or_else(|| decode("page cursor overflow"))?;
    let page = PageProgress {
        cursor,
        next,
        total,
    };
    validation::validate_page_progress(&page)?;
    Ok((next < total).then_some(next))
}
