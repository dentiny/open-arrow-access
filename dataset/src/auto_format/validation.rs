use open_arrow_access_core::{ObjectRef, Result, SchemaRef};

use super::{DetectedFormat, ResolvedKind, error};

pub(super) fn validate_files(objects: &[ObjectRef]) -> Result<()> {
    if objects.is_empty() {
        return Err(error::no_dataset_files());
    }
    Ok(())
}

pub(super) fn validate_schema(
    schema: Option<&SchemaRef>,
    kind: ResolvedKind,
    object: Option<&ObjectRef>,
) -> Result<()> {
    if schema.is_some() && !matches!(kind, ResolvedKind::CsvOrTsv | ResolvedKind::Json) {
        return Err(error::unsupported_schema(object));
    }
    Ok(())
}

pub(super) fn validate_same_format(
    first: DetectedFormat,
    example: &ObjectRef,
    other: DetectedFormat,
    object: &ObjectRef,
) -> Result<()> {
    if other != first {
        return Err(error::mixed_formats(first, example, other, object));
    }
    Ok(())
}
