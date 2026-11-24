//! Arrow scalar conversion and array construction.
use super::{
    error::invalid_value,
    validation::{validate_array, validate_values},
};
use arrow_array::{ArrayRef, StringArray};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::Field;
use open_arrow_access_core::Result;

pub(super) fn cast(input: &StringArray, field: &Field, context: &str) -> Result<ArrayRef> {
    validate_values(input, field, context)?;
    let output = cast_with_options(
        input,
        field.data_type(),
        &CastOptions {
            safe: false,
            ..Default::default()
        },
    )
    .map_err(|e| invalid_value(context, field).with_source(e))?;
    validate_array(output.as_ref(), field, context)?;
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, TimestampMicrosecondArray, UInt64Array};
    use arrow_schema::{DataType, TimeUnit};
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
