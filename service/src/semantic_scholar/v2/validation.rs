use super::error::error;
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

pub(super) fn parse_spans(input: &str) -> Result<Vec<Span>> {
    let spans: Vec<RawSpan> = serde_json::from_str(input)
        .map_err(|_| error("invalid annotation payload or unknown span/attribute field"))?;
    spans
        .into_iter()
        .map(|span| {
            Ok(Span {
                start: span.start.value()?,
                end: span.end.value()?,
                attributes: span.attributes,
            })
        })
        .collect()
}
