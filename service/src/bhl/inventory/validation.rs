use super::{BhlOcrScope, BhlPageIdentity, JobDocument, VERSION};
use crate::bhl::error::inventory as invalid;
use open_arrow_access_core::Result;
use std::collections::HashSet;

pub(super) fn header(version: u32, snapshot_id: &str) -> Result<()> {
    if version != VERSION || snapshot_id.trim().is_empty() {
        return Err(invalid(
            "unsupported inventory/job version or empty snapshot_id",
        ));
    }
    Ok(())
}

pub(super) fn objects(scope: BhlOcrScope, objects: &[String]) -> Result<()> {
    let mut identities = HashSet::new();
    for (index, key) in objects.iter().enumerate() {
        if index > 0 && objects[index - 1] >= *key {
            return Err(invalid("OCR keys must be unique and sorted"));
        }
        let identity = BhlPageIdentity::parse(key)?;
        if !scope.accepts(&identity) {
            return Err(invalid("OCR key is outside the declared namespace scope"));
        }
        if !identities.insert(identity) {
            return Err(invalid("duplicate logical OCR page identity"));
        }
    }
    Ok(())
}

pub(super) fn job(job: &JobDocument) -> Result<()> {
    let Some(start) = job.index.checked_mul(job.pages_per_job) else {
        return Err(invalid("job index overflows its page offset"));
    };
    if job.pages_per_job == 0
        || start >= job.inventory_pages
        || job.objects.len() != job.pages_per_job.min(job.inventory_pages - start)
    {
        return Err(invalid("job index, page count and sizing are inconsistent"));
    }
    Ok(())
}
