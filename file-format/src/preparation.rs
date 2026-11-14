use open_arrow_access_core::{ReadOptions, Result, SchemaRef};

/// Resolved output fields, independent of a format's per-reader decoder state.
pub(crate) struct PreparedProjection {
    pub(crate) schema: SchemaRef,
    #[cfg_attr(not(feature = "csv"), allow(dead_code))]
    pub(crate) indices: Option<Vec<usize>>,
}

impl PreparedProjection {
    pub(crate) fn new(source: &SchemaRef, options: &ReadOptions) -> Result<Self> {
        let (schema, indices) = options.resolve_projection(source)?;
        Ok(Self { schema, indices })
    }
}
