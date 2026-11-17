use super::{JsonKind, error};
use open_arrow_access_core::{ObjectRef, Result, SchemaRef};

pub(super) fn validate_same_kind(
    first: JsonKind,
    example: &ObjectRef,
    other: JsonKind,
    object: &ObjectRef,
) -> Result<()> {
    if other != first {
        return Err(error::mixed_kinds(first, example, other, object));
    }
    Ok(())
}

pub(super) fn validate_schema(
    schema: Option<&SchemaRef>,
    kind: JsonKind,
    object: Option<&ObjectRef>,
) -> Result<()> {
    if schema.is_some() && matches!(kind, JsonKind::Atif(_)) {
        return Err(error::unsupported_schema(object));
    }
    Ok(())
}
