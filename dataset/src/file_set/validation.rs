use open_arrow_access_core::{Error, ErrorKind, OrderRequirement, Result};

pub(super) fn validate_file_ordering(
    ordering: OrderRequirement,
    file_ordering: OrderRequirement,
) -> Result<()> {
    if ordering == OrderRequirement::Reorderable && file_ordering != OrderRequirement::Reorderable {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "plan_file_set",
            "file plan violates the accessor-wide reorderable contract",
        ));
    }
    Ok(())
}
