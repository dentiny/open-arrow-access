use super::error::error;
use arrow_array::{Array, ArrayRef, StructArray, UInt64Array, cast::AsArray};
use open_arrow_access_core::Result;
use serde::{Deserialize, Deserializer, de::Visitor};
use std::fmt;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Attributes {
    pub(super) n: Option<String>,
    pub(super) id: Option<String>,
    pub(super) ref_id: Option<String>,
    pub(super) matched_paper_id: Option<u64>,
}
enum Offset {
    Number(u64),
    InvalidDecimal,
    DecimalOverflow,
}
impl<'de> Deserialize<'de> for Offset {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct OffsetVisitor;
        impl Visitor<'_> for OffsetVisitor {
            type Value = Offset;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an unsigned integer or decimal string")
            }

            fn visit_u64<E: serde::de::Error>(self, value: u64) -> std::result::Result<Offset, E> {
                Ok(Offset::Number(value))
            }

            fn visit_str<E: serde::de::Error>(self, value: &str) -> std::result::Result<Offset, E> {
                // Keep validation errors separate from malformed annotation errors.
                if value.is_empty() || !value.bytes().all(|c| c.is_ascii_digit()) {
                    return Ok(Offset::InvalidDecimal);
                }
                Ok(value
                    .parse()
                    .map_or(Offset::DecimalOverflow, Offset::Number))
            }
        }
        deserializer.deserialize_any(OffsetVisitor)
    }
}
impl Offset {
    fn value(self) -> Result<u64> {
        match self {
            Self::Number(n) => Ok(n),
            Self::DecimalOverflow => Err(error("span offset overflows UInt64")),
            Self::InvalidDecimal => Err(error(
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

#[cfg(test)]
mod tests {
    use super::Offset;

    #[test]
    fn offsets_preserve_integer_string_boundaries_and_validation_errors() {
        for input in ["18446744073709551615", r#""18446744073709551615""#] {
            let offset: Offset = serde_json::from_str(input).unwrap();
            assert_eq!(offset.value().unwrap(), u64::MAX);
        }
        let escaped: Offset = serde_json::from_str(r#""\u0030\u0031""#).unwrap();
        assert_eq!(escaped.value().unwrap(), 1);
        for input in ["-1", "0.5", "null", "true", "18446744073709551616"] {
            assert!(serde_json::from_str::<Offset>(input).is_err(), "{input}");
        }
        for (input, message) in [
            (r#""""#, "unsigned integer or decimal string"),
            (r#""-1""#, "unsigned integer or decimal string"),
            (r#""1.5""#, "unsigned integer or decimal string"),
            (r#""18446744073709551616""#, "overflows UInt64"),
        ] {
            let offset: Offset = serde_json::from_str(input).unwrap();
            assert!(offset.value().unwrap_err().to_string().contains(message));
        }
    }
}
