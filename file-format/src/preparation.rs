use open_arrow_access_core::{ReadOptions, Result, SchemaRef};

pub(crate) use open_arrow_access_core::ResolvedProjection as PreparedProjection;

pub(crate) fn prepare_projection(
    source: &SchemaRef,
    options: &ReadOptions,
) -> Result<PreparedProjection> {
    options.resolve_projection(source)
}
