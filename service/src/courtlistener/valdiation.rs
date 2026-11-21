#[cfg(feature = "courtlistener-joined")]
use super::error::db_error;
use super::{
    CourtListenerCopyDialect, CourtListenerSchemas,
    error::{invalid_dialect, invalid_value, records_error, schema_error},
};
use arrow_array::{Array, Float64Array, StringArray};
use arrow_schema::{DataType, Field, TimeUnit};
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

impl CourtListenerSchemas {
    pub(crate) fn validate(&self) -> Result<()> {
        for fields in self.tables() {
            let mut seen = std::collections::HashSet::new();
            for f in fields {
                if !seen.insert(f.name())
                    || !matches!(
                        f.data_type(),
                        DataType::Utf8
                            | DataType::Boolean
                            | DataType::Int16
                            | DataType::Int32
                            | DataType::Int64
                            | DataType::UInt64
                            | DataType::Float64
                            | DataType::Date32
                            | DataType::Timestamp(TimeUnit::Microsecond, _)
                    )
                {
                    return Err(schema_error("duplicate field or unsupported schema type"));
                }
                if let DataType::Timestamp(_, zone) = f.data_type()
                    && zone.as_deref() != Some("UTC")
                {
                    return Err(schema_error("timestamps must use UTC"));
                }
            }
        }
        for (fields, name, dtype, required) in [
            (&self.opinion, "id", DataType::Int64, true),
            (&self.opinion, "cluster_id", DataType::Int64, true),
            (&self.cluster, "id", DataType::Int64, true),
            (&self.cluster, "docket_id", DataType::Int64, true),
            (&self.docket, "id", DataType::Int64, true),
            (&self.docket, "court_id", DataType::Utf8, true),
            (&self.court, "id", DataType::Utf8, true),
        ] {
            if !fields.iter().any(|f| {
                f.name() == name && f.data_type() == &dtype && (!required || !f.is_nullable())
            }) {
                return Err(schema_error(
                    "missing, nullable, or incompatible primary/join key",
                ));
            }
        }
        Ok(())
    }
}

impl CourtListenerCopyDialect {
    pub(crate) fn validate(self) -> Result<()> {
        for byte in [self.quote, self.escape] {
            if !byte.is_ascii_graphic() || matches!(byte, b',' | b'\r' | b'\n') {
                return Err(invalid_dialect());
            }
        }
        Ok(())
    }
}

pub(super) fn validate_values(input: &StringArray, field: &Field, context: &str) -> Result<()> {
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
            return Err(invalid_value(context, field));
        }
    }
    Ok(())
}

pub(super) fn validate_array(output: &dyn Array, field: &Field, context: &str) -> Result<()> {
    if !field.is_nullable() && output.null_count() != 0 {
        return Err(invalid_value(context, field));
    }
    if let Some(floats) = output.as_any().downcast_ref::<Float64Array>()
        && floats.iter().flatten().any(|v| !v.is_finite())
    {
        return Err(invalid_value(context, field));
    }
    Ok(())
}

fn digits(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

pub(super) fn validate_header(
    actual: Option<&Vec<Option<String>>>,
    expected: &[Option<String>],
    object: &str,
) -> Result<()> {
    if actual.map(Vec::as_slice) != Some(expected) {
        return Err(records_error(format!(
            "{object}: missing or mismatched CourtListener CSV header"
        )));
    }
    Ok(())
}
pub(super) fn validate_columns(
    actual: usize,
    expected: usize,
    object: &str,
    ordinal: u64,
) -> Result<()> {
    if actual != expected {
        return Err(records_error(format!(
            "{object} record {ordinal}: CSV column count differs from pinned schema"
        )));
    }
    Ok(())
}

#[cfg(feature = "courtlistener-joined")]
pub(super) fn validate_keys(connection: &duckdb::Connection, tables: &[&str]) -> Result<()> {
    for (i, table) in tables.iter().enumerate() {
        let duplicate = connection.query_row(
            &format!("SELECT 1 FROM cl_{i} GROUP BY id HAVING count(*)>1 LIMIT 1"),
            [],
            |row| row.get::<_, i32>(0),
        );
        match duplicate {
            Ok(_) => return Err(records_error(format!("duplicate primary key in {table}"))),
            Err(duckdb::Error::QueryReturnedNoRows) => {}
            Err(e) => return Err(db_error(e)),
        }
    }
    Ok(())
}

#[cfg(feature = "courtlistener-joined")]
pub(super) fn validate_join_options(options: &super::CourtListenerJoinOptions) -> Result<()> {
    options.dialect.validate()?;
    if options.source_root.is_empty() {
        return Err(invalid());
    }
    Ok(())
}
#[cfg(feature = "courtlistener-joined")]
pub(super) fn validate_join_schema(schemas: &CourtListenerSchemas) -> Result<()> {
    schemas.validate()?;
    if schemas
        .tables()
        .iter()
        .any(|fields| fields.iter().any(|f| f.name() == super::joined::ORDINAL))
    {
        return Err(records_error("source field collides with reserved ordinal"));
    }
    Ok(())
}
