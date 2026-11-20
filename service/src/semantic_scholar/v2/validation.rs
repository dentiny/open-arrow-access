use super::error::error;
use arrow_array::{Array, ArrayRef, StructArray, UInt64Array, cast::AsArray};
use open_arrow_access_core::Result;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Attributes {
    pub(super) n: Option<String>,
    pub(super) id: Option<String>,
    pub(super) ref_id: Option<String>,
    pub(super) matched_paper_id: Option<u64>,
}
#[derive(Deserialize)]
#[serde(untagged)]
enum Offset {
    Number(u64),
    Decimal(String),
}
impl Offset {
    fn value(self) -> Result<u64> {
        match self {
            Self::Number(n) => Ok(n),
            Self::Decimal(s) if !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit()) => {
                s.parse().map_err(|_| error("span offset overflows UInt64"))
            }
            _ => Err(error(
                "span offset must be an unsigned integer or decimal string",
            )),
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSpan {
    start: Offset,
    end: Offset,
    attributes: Option<Attributes>,
}

pub(super) struct Span {
    pub(super) start: u64,
    pub(super) end: u64,
    pub(super) attributes: Option<Attributes>,
}

pub(super) fn parse_spans(input: &str) -> Result<impl Iterator<Item = Result<Span>>> {
    let spans: Vec<RawSpan> = serde_json::from_str(input)
        .map_err(|_| error("invalid annotation payload or unknown span/attribute field"))?;
    Ok(spans.into_iter().map(|span| {
        let start = span.start.value()?;
        let end = span.end.value()?;
        if start > end {
            return Err(error("annotation start exceeds end"));
        }
        Ok(Span {
            start,
            end,
            attributes: span.attributes,
        })
    }))
}

pub(super) fn validate_ids(ids: &UInt64Array) -> Result<()> {
    if ids.iter().any(|id| id.is_none_or(|id| id == 0)) {
        return Err(error("corpusid must be a positive UInt64"));
    }
    Ok(())
}

pub(super) fn validate_section(input: &StructArray, arrays: &[ArrayRef]) -> Result<()> {
    let annotations = input["annotations"].as_struct();
    for row in 0..input.len() {
        if input.is_valid(row) && input["text"].is_null(row) && annotations.is_valid(row) {
            for array in arrays {
                let list = array.as_list::<i32>();
                if list.is_valid(row) && list.value_length(row) > 0 {
                    return Err(error("populated annotation spans require section text"));
                }
            }
        }
    }
    Ok(())
}
