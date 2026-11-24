use std::sync::Arc;

use arrow_array::{UInt64Array, cast::AsArray};
use arrow_schema::DataType;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// A row-level byte-length expression for Utf8/Binary, their Large and View
/// variants, and FixedSizeBinary. Null cells return null; empty cells return zero.
/// Counts payload bytes only, excluding Arrow offsets, validity and allocation
/// overhead. Binary data is never decoded as UTF-8.
///
/// ```
/// use datafusion::prelude::{col, lit};
/// use open_arrow_access_layers::{FilterLayer, cell_byte_length};
///
/// let filter = FilterLayer::new(vec![
///     cell_byte_length(col("data")).lt_eq(lit(1024_u64)),
/// ]);
/// ```
pub fn cell_byte_length(value: Expr) -> Expr {
    ScalarUDF::from(CellByteLength::new()).call(vec![value])
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CellByteLength {
    signature: Signature,
}

impl CellByteLength {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CellByteLength {
    fn name(&self) -> &str {
        "cell_byte_length"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> datafusion::error::Result<DataType> {
        match args {
            [
                DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
                | DataType::FixedSizeBinary(_),
            ] => Ok(DataType::UInt64),
            _ => datafusion::common::plan_err!(
                "cell_byte_length requires a string or binary cell, got {args:?}"
            ),
        }
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let array = &arrays[0];
        macro_rules! lengths {
            ($values:expr) => {
                $values
                    .iter()
                    .map(|value| value.map(|value| value.len() as u64))
                    .collect::<UInt64Array>()
            };
        }
        let lengths = match array.data_type() {
            DataType::Utf8 => lengths!(array.as_string::<i32>()),
            DataType::LargeUtf8 => lengths!(array.as_string::<i64>()),
            DataType::Utf8View => lengths!(array.as_string_view()),
            DataType::Binary => lengths!(array.as_binary::<i32>()),
            DataType::LargeBinary => lengths!(array.as_binary::<i64>()),
            DataType::BinaryView => lengths!(array.as_binary_view()),
            DataType::FixedSizeBinary(_) => lengths!(array.as_fixed_size_binary()),
            actual => {
                return datafusion::common::exec_err!(
                    "cell_byte_length requires a string or binary cell, got {actual}"
                );
            }
        };
        if matches!(args.args[0], ColumnarValue::Scalar(_)) {
            return Ok(ColumnarValue::Scalar(
                datafusion::common::ScalarValue::try_from_array(&lengths, 0)?,
            ));
        }
        Ok(ColumnarValue::Array(Arc::new(lengths)))
    }
}
