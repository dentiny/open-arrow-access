use std::collections::HashSet;

use arrow_array::types::{
    Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type,
    validate_decimal_precision_and_scale,
};
use arrow_cast::can_cast_types;
use arrow_schema::{ArrowError, DataType, Field, TimeUnit};
use open_arrow_access_core::{
    EmissionPolicy, FieldNameIndex, OrderRequirement, ParallelReadOptions, Result,
};

pub(crate) fn validate_sampling_probability(probability: f64) -> Result<()> {
    if !probability.is_finite() || !(0.0..=1.0).contains(&probability) {
        return Err(crate::error::invalid_probability());
    }
    Ok(())
}

pub(crate) fn validate_emission_policy(
    emission: EmissionPolicy,
    ordering: OrderRequirement,
) -> Result<()> {
    if emission == EmissionPolicy::Unordered && ordering != OrderRequirement::Reorderable {
        return Err(crate::error::invalid_emission());
    }
    Ok(())
}

pub(crate) fn validate_parallel_read_settings(
    previous: Option<ParallelReadOptions>,
    settings: ParallelReadOptions,
) -> Result<()> {
    if previous.is_some_and(|previous| previous != settings) {
        return Err(crate::error::conflicting_parallel_settings());
    }
    Ok(())
}

pub(crate) fn validate_cast(field: &Field, target: &DataType) -> Result<()> {
    let from = field.data_type();
    if has_extension(field) || has_extension_type(target) {
        return Err(crate::error::extension_cast(field));
    }
    if !can_cast_types(from, target) {
        return Err(crate::error::unsupported_cast(field, from, target));
    }
    validate_decimal_cast(from, target)
        .map_err(|error| crate::error::cast_plan_error(field.name(), error))
}

fn decimal_params(data_type: &DataType) -> Option<(i16, i16)> {
    match data_type {
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            Some((i16::from(*precision), i16::from(*scale)))
        }
        _ => None,
    }
}

fn list_item(data_type: &DataType) -> Option<&DataType> {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => Some(field.data_type()),
        _ => None,
    }
}

fn validate_decimal_cast(from: &DataType, to: &DataType) -> std::result::Result<(), ArrowError> {
    if from == to || from.is_null() {
        return Ok(());
    }
    if let (Some((precision, from_scale)), Some((_, to_scale))) =
        (decimal_params(from), decimal_params(to))
    {
        // Arrow 59.3 computes both expressions in i8, which can panic or select wrapping multiplication.
        let delta = to_scale - from_scale;
        if delta.abs() > i16::from(i8::MAX)
            || (delta >= 0 && precision + delta > i16::from(i8::MAX))
        {
            return Err(crate::error::unsafe_decimal_rescale(from, to));
        }
        return Ok(());
    }
    match (from, to) {
        (DataType::RunEndEncoded(_, values), _) => validate_decimal_cast(values.data_type(), to)?,
        (DataType::Union(fields, _), _) => {
            if !fields.iter().any(|(_, field)| field.data_type() == to)
                && let Some((_, field)) = fields
                    .iter()
                    .find(|(_, field)| can_cast_types(field.data_type(), to))
            {
                validate_decimal_cast(field.data_type(), to)?;
            }
        }
        (DataType::Dictionary(_, from), DataType::Dictionary(_, to)) => {
            let mut from = from.as_ref();
            while let DataType::Dictionary(_, values) = from {
                from = values;
            }
            validate_decimal_cast(from, to)?;
        }
        (DataType::Dictionary(_, value), _) => validate_decimal_cast(value, to)?,
        (_, DataType::Dictionary(_, value)) => validate_decimal_cast(from, value)?,
        (DataType::Struct(from_fields), DataType::Struct(to_fields)) => {
            let in_order = from_fields
                .iter()
                .zip(to_fields)
                .all(|(from, to)| from.name() == to.name());
            let by_name = (!in_order)
                .then(|| FieldNameIndex::new(from_fields))
                .filter(|index| {
                    to_fields
                        .iter()
                        .all(|field| index.get(field.name()).is_some())
                });
            for (position, to) in to_fields.iter().enumerate() {
                let index = by_name
                    .as_ref()
                    .map_or(position, |index| index.get(to.name()).unwrap());
                let from = &from_fields[index];
                validate_decimal_cast(from.data_type(), to.data_type())?;
            }
        }
        (DataType::Map(from, _), DataType::Map(to, _)) => {
            if let (DataType::Struct(from), DataType::Struct(to)) =
                (from.data_type(), to.data_type())
            {
                for (from, to) in from.iter().zip(to) {
                    validate_decimal_cast(from.data_type(), to.data_type())?;
                }
            }
        }
        _ => match (list_item(from), list_item(to)) {
            (Some(from), Some(to)) => validate_decimal_cast(from, to)?,
            (_, Some(to)) => validate_decimal_cast(from, to)?,
            (Some(item), None) if matches!(from, DataType::FixedSizeList(_, 1)) => {
                validate_decimal_cast(item, to)?
            }
            _ => {}
        },
    }
    Ok(())
}

fn validate_evolution_type(
    data_type: &DataType,
    for_cast: bool,
) -> std::result::Result<(), ArrowError> {
    let invalid = || crate::error::invalid_evolution_type(data_type);
    match data_type {
        DataType::Time32(unit) if !matches!(unit, TimeUnit::Second | TimeUnit::Millisecond) => {
            return Err(invalid());
        }
        DataType::Time64(unit) if !matches!(unit, TimeUnit::Microsecond | TimeUnit::Nanosecond) => {
            return Err(invalid());
        }
        DataType::FixedSizeBinary(size) | DataType::FixedSizeList(_, size) if *size < 0 => {
            return Err(invalid());
        }
        DataType::Dictionary(key, _) if !key.is_dictionary_key_type() => return Err(invalid()),
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields)
                if !entries.is_nullable() && fields.len() == 2 && !fields[0].is_nullable() => {}
            _ => return Err(invalid()),
        },
        DataType::Union(fields, _) => {
            let mut ids = HashSet::with_capacity(fields.len());
            if (for_cast && fields.is_empty())
                || fields.iter().any(|(id, _)| id < 0 || !ids.insert(id))
            {
                return Err(invalid());
            }
        }
        DataType::RunEndEncoded(run_ends, values) => {
            // Arrow's null-to-run-end cast can panic beyond the key range.
            // Expression-provided arrays do not use that cast path.
            if for_cast || run_ends.is_nullable() || !run_ends.data_type().is_run_ends_type() {
                return Err(invalid());
            }
            return validate_evolution_type(values.data_type(), for_cast);
        }
        DataType::Decimal32(precision, scale) => {
            return validate_decimal_precision_and_scale::<Decimal32Type>(*precision, *scale);
        }
        DataType::Decimal64(precision, scale) => {
            return validate_decimal_precision_and_scale::<Decimal64Type>(*precision, *scale);
        }
        DataType::Decimal128(precision, scale) => {
            return validate_decimal_precision_and_scale::<Decimal128Type>(*precision, *scale);
        }
        DataType::Decimal256(precision, scale) => {
            return validate_decimal_precision_and_scale::<Decimal256Type>(*precision, *scale);
        }
        _ => {}
    }
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => validate_evolution_type(field.data_type(), for_cast)?,
        DataType::Struct(fields) => {
            for field in fields {
                validate_evolution_type(field.data_type(), for_cast)?;
            }
        }
        DataType::Union(fields, _) => {
            for (_, field) in fields.iter() {
                validate_evolution_type(field.data_type(), for_cast)?;
            }
        }
        DataType::Dictionary(_, value) => validate_evolution_type(value, for_cast)?,
        _ => {}
    }
    Ok(())
}

fn has_extension(field: &Field) -> bool {
    field.metadata().contains_key("ARROW:extension:name") || has_extension_type(field.data_type())
}

fn has_extension_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => has_extension(field),
        DataType::Struct(fields) => fields.iter().any(|field| has_extension(field)),
        DataType::Union(fields, _) => fields.iter().any(|(_, field)| has_extension(field)),
        DataType::Dictionary(_, value) => has_extension_type(value),
        DataType::RunEndEncoded(ends, values) => has_extension(ends) || has_extension(values),
        _ => false,
    }
}

pub(crate) fn validate_generated_specs(specs: &[crate::GeneratedColumnSpec]) -> Result<()> {
    let mut names = HashSet::with_capacity(specs.len());
    for spec in specs {
        if spec.column.trim().is_empty() || !names.insert(&spec.column) {
            return Err(crate::error::invalid_generated_column(
                &spec.column,
                "column names must be nonempty and distinct",
            ));
        }
        if spec.expression.trim().is_empty() {
            return Err(crate::error::invalid_generated_column(
                &spec.column,
                "expression must not be blank",
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_expression(
    expression: &datafusion::logical_expr::Expr,
) -> datafusion::error::Result<()> {
    use datafusion::{
        common::tree_node::{TreeNode, TreeNodeRecursion},
        logical_expr::Expr,
    };

    expression.apply(|expr| {
        if expr.is_volatile_node() {
            return Err(crate::error::volatile_expression());
        }
        if matches!(
            expr,
            Expr::AggregateFunction(_)
                | Expr::WindowFunction(_)
                | Expr::ScalarSubquery(_)
                | Expr::Exists(_)
                | Expr::InSubquery(_)
                | Expr::SetComparison(_)
                | Expr::GroupingSet(_)
                | Expr::Unnest(_)
        ) {
            return Err(crate::error::non_row_expression());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

pub(crate) fn validate_expression_casts(
    expression: &datafusion::logical_expr::Expr,
    schema: &datafusion::common::DFSchema,
) -> datafusion::error::Result<()> {
    use datafusion::{
        common::tree_node::{TreeNode, TreeNodeRecursion},
        functions::core::{
            arrow_cast::ArrowCastFunc, arrow_try_cast::ArrowTryCastFunc,
            cast_to_type::CastToTypeFunc, try_cast_to_type::TryCastToTypeFunc,
        },
        logical_expr::{Expr, ExprSchemable},
    };

    expression.apply(|expr| {
        let source = match expr {
            Expr::Cast(cast) => Some(cast.expr.as_ref()),
            Expr::TryCast(cast) => Some(cast.expr.as_ref()),
            Expr::ScalarFunction(function) => {
                let function_impl = function.func.inner();
                (function_impl.downcast_ref::<ArrowCastFunc>().is_some()
                    || function_impl.downcast_ref::<ArrowTryCastFunc>().is_some()
                    || function_impl.downcast_ref::<CastToTypeFunc>().is_some()
                    || function_impl.downcast_ref::<TryCastToTypeFunc>().is_some())
                .then(|| function.args.first())
                .flatten()
            }
            _ => None,
        };
        if let Some(source) = source {
            let from = source.get_type(schema)?;
            let to = expr.get_type(schema)?;
            // Check before simplification can fold constants or lower cast UDFs.
            if can_cast_types(&from, &to) {
                validate_decimal_cast(&from, &to)?;
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

pub(crate) fn validate_generated_names(
    specs: &[crate::GeneratedColumnSpec],
    source: &arrow_schema::Schema,
) -> Result<()> {
    let mut names: HashSet<&str> = source
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    for spec in specs {
        if !names.insert(&spec.column) {
            return Err(crate::error::invalid_generated_column(
                &spec.column,
                "column already exists",
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_generated_schema(
    actual: &arrow_schema::Schema,
    expected: &arrow_schema::Schema,
) -> Result<()> {
    if actual != expected {
        return Err(crate::error::generated_schema_mismatch());
    }
    Ok(())
}

pub(crate) fn validate_partition_schema(
    actual: &arrow_schema::SchemaRef,
    expected: &arrow_schema::SchemaRef,
) -> Result<()> {
    if actual != expected {
        return Err(crate::error::schema_error());
    }
    Ok(())
}

pub(crate) fn validate_evolution_specs(changes: &[crate::ColumnEvolution]) -> Result<()> {
    use crate::ColumnEvolution;
    for change in changes {
        let name = match change {
            ColumnEvolution::RemoveColumn { name } => name,
            ColumnEvolution::AddColumn {
                name, data_type, ..
            } => {
                validate_evolution_type(data_type, false)
                    .map_err(|error| crate::error::evolution_config_error(name, error))?;
                name
            }
            ColumnEvolution::RenameColumn {
                source_name,
                target_name,
            } => {
                validate_evolution_name(target_name)?;
                source_name
            }
            ColumnEvolution::ChangeDataType { name, data_type } => {
                validate_evolution_type(data_type, true)
                    .map_err(|error| crate::error::evolution_config_error(name, error))?;
                name
            }
        };
        validate_evolution_name(name)?;
    }
    Ok(())
}

fn validate_evolution_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(crate::error::invalid_evolution(
            "column name must be nonempty",
        ));
    }
    Ok(())
}

pub(crate) fn validate_evolution_source(indexed_names: usize, source_fields: usize) -> Result<()> {
    if indexed_names != source_fields {
        return Err(crate::error::evolution_output_collision());
    }
    Ok(())
}

pub(crate) fn validate_evolution_target(
    source: Option<usize>,
    target: Option<usize>,
) -> Result<()> {
    if target.is_some() && target != source {
        return Err(crate::error::evolution_output_collision());
    }
    Ok(())
}

// The upstream RecordReader owns the fixed-schema contract for untouched fields.
pub(crate) fn validate_evolution_batch_schema(
    actual: &arrow_schema::SchemaRef,
    expected: &arrow_schema::SchemaRef,
    affected: &[usize],
) -> Result<()> {
    if !std::sync::Arc::ptr_eq(actual, expected)
        && (actual.fields().len() != expected.fields().len()
            || affected
                .iter()
                .any(|&index| actual.field(index) != expected.field(index)))
    {
        return Err(crate::error::evolution_schema_mismatch());
    }
    Ok(())
}

pub(crate) fn validate_added_column(
    name: &str,
    array: &dyn arrow_array::Array,
    expected: &DataType,
    rows: usize,
) -> Result<()> {
    if array.data_type() != expected {
        return Err(crate::error::invalid_added_column(
            name,
            format!("expected type {expected}, got {}", array.data_type()),
        ));
    }
    if array.len() != rows {
        return Err(crate::error::invalid_added_column(
            name,
            format!("expected {rows} rows, got {}", array.len()),
        ));
    }
    Ok(())
}

pub(crate) fn validate_evolution_expression_type(
    name: &str,
    actual: &DataType,
    expected: &DataType,
) -> Result<()> {
    if actual != expected {
        return Err(crate::error::evolution_expression_type(
            name, expected, actual,
        ));
    }
    Ok(())
}

pub(crate) fn validate_evolution_input_schema(
    actual: &arrow_schema::SchemaRef,
    expected: &arrow_schema::SchemaRef,
) -> Result<()> {
    if actual != expected {
        return Err(crate::error::evolution_schema_mismatch());
    }
    Ok(())
}
