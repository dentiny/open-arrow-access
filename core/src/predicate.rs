/// A conservative pruning hint, not a row filter. Readers may ignore it or return
/// whole matching partitions. Consumers must still evaluate the original filter.
/// Names refer to unprojected, top-level input fields. Unsupported types or
/// missing statistics must never cause rows to be discarded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PruningPredicate {
    /// Compare a non-null value with a column.
    Compare {
        column: String,
        op: CompareOp,
        value: PredicateValue,
    },
    /// Select null values.
    IsNull(String),
    /// Select non-null values.
    IsNotNull(String),
    /// All children must match. An empty conjunction matches everything.
    And(Vec<Self>),
    /// Any child may match. An empty disjunction matches nothing.
    Or(Vec<Self>),
}

/// SQL comparison with nulls excluded; no implicit type conversions are made.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

/// Exactly represented constants supported by statistics pruning. Floating point,
/// decimal and temporal comparisons deliberately remain residual filters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PredicateValue {
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    /// UTF-8 using binary, case-sensitive ordering, without collation.
    Utf8(String),
}
