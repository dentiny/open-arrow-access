use open_arrow_access_core::{
    EmissionPolicy, Error, ErrorKind, OrderRequirement, Result, ScanPlan,
};

// Execution policies belong to the enclosing dataset scan. Flattening a child
// policy would otherwise silently discard settings or introduce another budget.
pub(super) fn validate_file_policy(plan: &ScanPlan) -> Result<()> {
    if plan.parallel_read.is_some() || plan.emission != EmissionPolicy::Ordered {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "plan_file_set",
            "place execution and emission layers around the complete file set",
        ));
    }
    Ok(())
}

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
