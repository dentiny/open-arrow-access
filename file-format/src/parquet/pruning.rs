use std::cmp::Ordering;

use arrow_schema::{DataType, Schema};
use open_arrow_access_core::{CompareOp, PredicateValue, PruningPredicate};
use parquet::file::{metadata::RowGroupMetaData, statistics::Statistics};

/// False means statistics prove no row can satisfy the predicate. Unknown always
/// means keep the group. The caller remains responsible for filtering its rows.
pub(super) fn may_match(
    predicate: &PruningPredicate,
    schema: &Schema,
    group: &RowGroupMetaData,
) -> bool {
    use PruningPredicate::*;
    match predicate {
        And(children) => children.iter().all(|p| may_match(p, schema, group)),
        Or(children) => children.iter().any(|p| may_match(p, schema, group)),
        Compare { column, op, value } => {
            let Some((data_type, statistics)) = column_stats(column, schema, group) else {
                return true;
            };
            if matches!((statistics.null_count_opt(), u64::try_from(group.num_rows())), (Some(nulls), Ok(rows)) if nulls == rows)
            {
                return false;
            }
            // Legacy ordering and truncated/missing bounds are not trustworthy
            // for all logical types. Fall back to reading instead of guessing.
            if statistics.is_min_max_deprecated()
                || !statistics.min_is_exact()
                || !statistics.max_is_exact()
            {
                return true;
            }
            let Some((min, max, value)) = bounds(data_type, statistics, value) else {
                return true;
            };
            if min > max {
                return true;
            }
            let lo = min.cmp(&value);
            let hi = max.cmp(&value);
            use CompareOp::*;
            match op {
                Eq => lo != Ordering::Greater && hi != Ordering::Less,
                NotEq => lo != Ordering::Equal || hi != Ordering::Equal,
                Lt => lo == Ordering::Less,
                LtEq => lo != Ordering::Greater,
                Gt => hi == Ordering::Greater,
                GtEq => hi != Ordering::Less,
            }
        }
        IsNull(column) => column_stats(column, schema, group)
            .is_none_or(|(_, stats)| stats.null_count_opt() != Some(0)),
        IsNotNull(column) => column_stats(column, schema, group).is_none_or(|(_, stats)| {
            match (stats.null_count_opt(), u64::try_from(group.num_rows())) {
                (Some(nulls), Ok(rows)) => nulls != rows,
                _ => true,
            }
        }),
    }
}

fn column_stats<'a>(
    column: &str,
    schema: &'a Schema,
    group: &'a RowGroupMetaData,
) -> Option<(&'a DataType, &'a Statistics)> {
    // A nested leaf with the same name must never stand in for a root field.
    let field = schema.field_with_name(column).ok()?;
    let chunk = group
        .columns()
        .iter()
        .find(|chunk| chunk.column_path().parts() == [column])?;
    Some((field.data_type(), chunk.statistics()?))
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum Value<'a> {
    Integer(i128),
    Boolean(bool),
    Bytes(&'a [u8]),
}

fn bounds<'a>(
    data_type: &DataType,
    stats: &'a Statistics,
    value: &'a PredicateValue,
) -> Option<(Value<'a>, Value<'a>, Value<'a>)> {
    use DataType::*;
    let integers = |min, max, value| {
        Some((
            Value::Integer(min),
            Value::Integer(max),
            Value::Integer(value),
        ))
    };
    match (data_type, stats, value) {
        (Int8 | Int16 | Int32, Statistics::Int32(s), PredicateValue::Int64(value)) => integers(
            i128::from(*s.min_opt()?),
            i128::from(*s.max_opt()?),
            i128::from(*value),
        ),
        (Int64, Statistics::Int64(s), PredicateValue::Int64(value)) => integers(
            i128::from(*s.min_opt()?),
            i128::from(*s.max_opt()?),
            i128::from(*value),
        ),
        (UInt8 | UInt16 | UInt32, Statistics::Int32(s), PredicateValue::UInt64(value)) => integers(
            i128::from(*s.min_opt()? as u32),
            i128::from(*s.max_opt()? as u32),
            i128::from(*value),
        ),
        (UInt64, Statistics::Int64(s), PredicateValue::UInt64(value)) => integers(
            i128::from(*s.min_opt()? as u64),
            i128::from(*s.max_opt()? as u64),
            i128::from(*value),
        ),
        (Boolean, Statistics::Boolean(s), PredicateValue::Boolean(value)) => Some((
            Value::Boolean(*s.min_opt()?),
            Value::Boolean(*s.max_opt()?),
            Value::Boolean(*value),
        )),
        (Utf8 | LargeUtf8 | Utf8View, Statistics::ByteArray(s), PredicateValue::Utf8(value)) => {
            Some((
                Value::Bytes(s.min_opt()?.data()),
                Value::Bytes(s.max_opt()?.data()),
                Value::Bytes(value.as_bytes()),
            ))
        }
        _ => None,
    }
}
