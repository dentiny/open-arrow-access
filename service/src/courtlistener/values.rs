use arrow_array::{
    ArrayRef, BooleanArray, Date32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    StringArray, StructArray, TimestampMicrosecondArray, UInt64Array,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Fields, TimeUnit};
use chrono::{DateTime, NaiveDate};
use open_arrow_access_core::{Error, ErrorKind, Result};
use serde_json::Value;
use std::sync::Arc;

pub(super) type Row = Vec<Value>;
pub(super) fn decode(raw: Vec<Option<String>>, fields: &Fields, context: &str) -> Result<Row> {
    if raw.len() != fields.len() {
        return Err(error(format!(
            "{context}: CSV column count differs from pinned schema"
        )));
    }
    raw.into_iter()
        .zip(fields)
        .map(|(raw, f)| {
            let context = format!("{context} field {}", f.name());
            let Some(s) = raw else {
                return if f.is_nullable() {
                    Ok(Value::Null)
                } else {
                    Err(error(format!("{context}: NULL in required field")))
                };
            };
            let invalid = || error(format!("{context}: invalid {}", f.data_type()));
            let signed = || -> Result<i64> {
                if s.is_empty()
                    || s.starts_with('+')
                    || s.trim() != s
                    || !s
                        .trim_start_matches('-')
                        .bytes()
                        .all(|b| b.is_ascii_digit())
                {
                    return Err(invalid());
                }
                s.parse::<i64>().map_err(|_| invalid())
            };
            let v = match f.data_type() {
                DataType::Utf8 => Value::String(s),
                DataType::Boolean => match s.as_str() {
                    "t" => Value::Bool(true),
                    "f" => Value::Bool(false),
                    _ => return Err(invalid()),
                },
                DataType::Int16 => Value::from(i16::try_from(signed()?).map_err(|_| invalid())?),
                DataType::Int32 => Value::from(i32::try_from(signed()?).map_err(|_| invalid())?),
                DataType::Int64 => Value::from(signed()?),
                DataType::UInt64 => {
                    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
                        return Err(invalid());
                    }
                    Value::from(s.parse::<u64>().map_err(|_| invalid())?)
                }
                DataType::Float64 => {
                    let v = s.parse::<f64>().map_err(|_| invalid())?;
                    if !v.is_finite() || s.trim() != s {
                        return Err(invalid());
                    }
                    Value::from(v)
                }
                DataType::Date32 => Value::from(date(&s).map_err(|_| invalid())?),
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    // PostgreSQL COPY emits a space separator and may use +00 / +0000 offsets.
                    let mut text = s.replacen(' ', "T", 1);
                    let tail = text.rsplit('T').next().unwrap_or("");
                    if let Some(i) = tail.rfind(['+', '-']) {
                        let n = tail.len() - i;
                        if n == 3 {
                            text.push_str(":00");
                        } else if n == 5 {
                            text.insert(text.len() - 2, ':');
                        }
                    }
                    if let Some((_, fraction)) = text.split_once('.')
                        && fraction
                            .bytes()
                            .take_while(u8::is_ascii_digit)
                            .skip(6)
                            .any(|b| b != b'0')
                    {
                        return Err(invalid());
                    }
                    let dt = DateTime::parse_from_rfc3339(&text).map_err(|_| invalid())?;
                    if dt.timestamp_subsec_nanos() % 1000 != 0
                        || dt.timestamp_subsec_nanos() >= 1_000_000_000
                    {
                        return Err(invalid());
                    }
                    Value::from(dt.timestamp_micros())
                }
                _ => return Err(invalid()),
            };
            if f.name() == "id" && v.as_str() == Some("") {
                return Err(invalid());
            }
            Ok(v)
        })
        .collect()
}
pub(super) fn date(s: &str) -> Result<i32> {
    if s.len() != 10 || s.as_bytes()[4] != b'-' || s.as_bytes()[7] != b'-' {
        return Err(error("invalid date"));
    }
    let d = NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|_| error("invalid date"))?;
    if d.format("%Y-%m-%d").to_string() != s {
        return Err(error("invalid date"));
    }
    i32::try_from(
        d.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days(),
    )
    .map_err(|_| error("date overflow"))
}
pub(super) fn structure(fields: &Fields, rows: &[Option<&Row>]) -> Result<ArrayRef> {
    let arrays = fields
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let values = rows
                .iter()
                .map(|row| row.map(|r| &r[i]).filter(|v| !v.is_null()))
                .collect::<Vec<_>>();
            let array: ArrayRef = match f.data_type() {
                DataType::Utf8 => Arc::new(StringArray::from(
                    values
                        .iter()
                        .map(|v| v.and_then(Value::as_str))
                        .collect::<Vec<_>>(),
                )),
                DataType::Boolean => Arc::new(BooleanArray::from(
                    values
                        .iter()
                        .map(|v| v.and_then(Value::as_bool))
                        .collect::<Vec<_>>(),
                )),
                DataType::Int16 => Arc::new(Int16Array::from(
                    values
                        .iter()
                        .map(|v| v.and_then(Value::as_i64).map(|v| v as i16))
                        .collect::<Vec<_>>(),
                )),
                DataType::Int32 => Arc::new(Int32Array::from(
                    values
                        .iter()
                        .map(|v| v.and_then(Value::as_i64).map(|v| v as i32))
                        .collect::<Vec<_>>(),
                )),
                DataType::Int64 => Arc::new(Int64Array::from(
                    values
                        .iter()
                        .map(|v| v.and_then(Value::as_i64))
                        .collect::<Vec<_>>(),
                )),
                DataType::UInt64 => Arc::new(UInt64Array::from(
                    values
                        .iter()
                        .map(|v| v.and_then(Value::as_u64))
                        .collect::<Vec<_>>(),
                )),
                DataType::Float64 => Arc::new(Float64Array::from(
                    values
                        .iter()
                        .map(|v| v.and_then(Value::as_f64))
                        .collect::<Vec<_>>(),
                )),
                DataType::Date32 => Arc::new(Date32Array::from(
                    values
                        .iter()
                        .map(|v| v.and_then(Value::as_i64).map(|v| v as i32))
                        .collect::<Vec<_>>(),
                )),
                DataType::Timestamp(TimeUnit::Microsecond, _) => Arc::new(
                    TimestampMicrosecondArray::from(
                        values
                            .iter()
                            .map(|v| v.and_then(Value::as_i64))
                            .collect::<Vec<_>>(),
                    )
                    .with_timezone("UTC"),
                ),
                _ => return Err(error("unsupported output type")),
            };
            Ok(array)
        })
        .collect::<Result<Vec<_>>>()?;
    let validity = rows.iter().map(Option::is_some).collect::<Vec<_>>();
    Ok(Arc::new(
        StructArray::try_new(fields.clone(), arrays, Some(NullBuffer::from(validity)))
            .map_err(|e| error(e.to_string()))?,
    ))
}
pub(super) fn error(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Decode, "courtlistener_records", message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    fn parse(typ: DataType, nullable: bool, value: Option<&str>) -> Result<Value> {
        Ok(decode(
            vec![value.map(str::to_owned)],
            &vec![Field::new("value", typ, nullable)].into(),
            "test record 0",
        )?
        .remove(0))
    }
    #[test]
    fn typed_values_retain_precision_and_reject_silent_casts() {
        let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        assert_eq!(
            parse(ts.clone(), false, Some("2026-09-30 05:30:00.123456+05:30")).unwrap(),
            parse(ts.clone(), false, Some("2026-09-30T00:00:00.123456Z")).unwrap()
        );
        assert_eq!(
            parse(ts.clone(), false, Some("1969-12-31 23:59:59.999999+00")).unwrap(),
            Value::from(-1)
        );
        for bad in ["2026-09-30 00:00:00", "2026-09-30T00:00:00.123456789Z"] {
            assert!(parse(ts.clone(), true, Some(bad)).is_err());
        }
        assert_eq!(
            parse(DataType::UInt64, true, Some("18446744073709551615")).unwrap(),
            Value::from(u64::MAX)
        );
        for bad in ["-1", "1.1", "18446744073709551616", " 1", ""] {
            assert!(parse(DataType::UInt64, true, Some(bad)).is_err());
        }
        assert!(parse(DataType::Int16, false, Some("32768")).is_err());
        assert!(parse(DataType::Boolean, true, Some("true")).is_err());
        assert_eq!(
            parse(DataType::Boolean, false, Some("t")).unwrap(),
            Value::Bool(true)
        );
        assert!(parse(DataType::Float64, false, Some("NaN")).is_err());
        assert!(parse(DataType::Date32, false, Some("2026-02-29")).is_err());
        assert_eq!(
            parse(DataType::Utf8, false, Some("001")).unwrap(),
            Value::from("001")
        );
        assert_eq!(
            parse(DataType::Utf8, false, Some("")).unwrap(),
            Value::from("")
        );
        assert!(parse(DataType::Utf8, false, None).is_err());
        assert_eq!(parse(DataType::UInt64, true, None).unwrap(), Value::Null);
    }
}
