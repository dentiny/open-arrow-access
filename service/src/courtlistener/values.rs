//! Arrow owns scalar conversion and array construction; this module only checks COPY semantics.
use arrow_array::{Array, ArrayRef, Float64Array, StringArray};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{DataType, Field};
use open_arrow_access_core::{Error, ErrorKind, Result};

pub(super) fn cast(input: &StringArray, field: &Field, context: &str) -> Result<ArrayRef> {
    let invalid = || {
        error(format!(
            "{context} field {}: invalid {}",
            field.name(),
            field.data_type()
        ))
    };
    for value in input.iter().flatten() {
        let valid = match field.data_type() {
            DataType::Boolean => matches!(value, "t" | "f"),
            DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                digits(value.strip_prefix('-').unwrap_or(value))
            }
            DataType::UInt64 => digits(value),
            DataType::Date32 => {
                value.len() == 10 && value.as_bytes()[4] == b'-' && value.as_bytes()[7] == b'-'
            }
            DataType::Timestamp(_, _) => {
                let time = value.get(11..).unwrap_or("");
                let has_zone = time.ends_with('Z') || time.contains(['+', '-']);
                let precision = time.split_once('.').is_none_or(|(_, fraction)| {
                    !fraction
                        .bytes()
                        .take_while(u8::is_ascii_digit)
                        .skip(6)
                        .any(|b| b != b'0')
                });
                has_zone && precision
            }
            _ => true,
        };
        if !valid
            || (field.data_type() != &DataType::Utf8 && value.trim() != value)
            || (field.name() == "id" && value.is_empty())
        {
            return Err(invalid());
        }
    }
    let output = cast_with_options(
        input,
        field.data_type(),
        &CastOptions {
            safe: false,
            ..Default::default()
        },
    )
    .map_err(|e| invalid().with_source(e))?;
    if !field.is_nullable() && output.null_count() != 0 {
        return Err(invalid());
    }
    if let Some(floats) = output.as_any().downcast_ref::<Float64Array>()
        && floats.iter().flatten().any(|v| !v.is_finite())
    {
        return Err(invalid());
    }
    Ok(output)
}
fn digits(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}
pub(super) fn error(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Decode, "courtlistener_records", message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{TimestampMicrosecondArray, UInt64Array};
    use arrow_schema::TimeUnit;
    #[test]
    fn arrow_cast_is_strict_and_preserves_copy_types_and_timestamp_precision() {
        let ts = Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        );
        let output = cast(
            &StringArray::from(vec![
                "2026-09-30 05:30:00.123456+05:30",
                "2026-09-30 00:00:00.123456+00",
                "1969-12-31 23:59:59.999999+00",
            ]),
            &ts,
            "test",
        )
        .unwrap();
        let values = output
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(values.value(0), values.value(1));
        assert_eq!(values.value(2), -1);
        let count = Field::new("count", DataType::UInt64, true);
        let output = cast(
            &StringArray::from(vec![Some("18446744073709551615"), None]),
            &count,
            "test",
        )
        .unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            u64::MAX
        );
        assert!(output.is_null(1));
        for (field, bad) in [
            (&ts, "2026-09-30 00:00:00"),
            (&ts, "2026-09-30T00:00:00.123456789Z"),
            (&count, "-1"),
            (&count, "1.1"),
            (&count, "18446744073709551616"),
            (&count, ""),
            (&Field::new("v", DataType::Int16, false), "32768"),
            (&Field::new("v", DataType::Boolean, false), "true"),
            (&Field::new("v", DataType::Float64, false), "NaN"),
            (&Field::new("v", DataType::Date32, false), "2026-02-29"),
        ] {
            assert!(
                cast(&StringArray::from(vec![bad]), field, "test").is_err(),
                "{bad}"
            );
        }
        let required = Field::new("text", DataType::Utf8, false);
        assert!(cast(&StringArray::from(vec![None::<&str>]), &required, "test").is_err());
        assert_eq!(
            cast(&StringArray::from(vec!["001", ""]), &required, "test")
                .unwrap()
                .len(),
            2
        );
    }
}
