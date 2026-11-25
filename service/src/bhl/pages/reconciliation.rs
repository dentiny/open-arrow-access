use crate::bhl::{error, metadata::BhlPageSelection};
use open_arrow_access_core::{
    ByteAccessor, ByteReadOptions, ByteReadRequest, ObjectRef, Result, read_object,
};
use std::sync::Arc;

/// An auditable decision made while reconciling OCR objects with metadata.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum BhlPageReconciliation {
    /// The selected object has no metadata-authoritative sequence match.
    /// Its OCR bytes are not read and it does not contribute an output row.
    Quarantined {
        object: ObjectRef,
        reason: &'static str,
        missing: Vec<String>,
        conflicts: Vec<String>,
    },
    /// Two metadata-authoritative aliases have byte-for-byte identical OCR.
    /// Only the first selected object contributes an output row.
    Deduplicated {
        canonical: ObjectRef,
        duplicate: ObjectRef,
    },
}

pub(in crate::bhl) type ReconciliationReport =
    Arc<dyn Fn(&BhlPageReconciliation) -> Result<()> + Send + Sync>;

pub(in crate::bhl) fn log_reconciliation(event: &BhlPageReconciliation) -> Result<()> {
    eprintln!("BHL reconciliation: {event:?}");
    Ok(())
}

pub(super) async fn read_canonical<A: ByteAccessor>(
    storage: &A,
    selection: &BhlPageSelection,
    options: ByteReadOptions,
    report: &ReconciliationReport,
) -> Result<Vec<u8>> {
    let canonical = &selection.page.object;
    let data = read_object(
        storage,
        ByteReadRequest::new(canonical.clone()).with_options(options),
        "read_bhl_page",
    )
    .await?;
    // At most the canonical body and one alternative body are retained. Verify
    // every alias before reporting deduplication or emitting the canonical row.
    for object in selection.objects.iter().skip(1) {
        if object == canonical {
            continue;
        }
        let alternative = read_object(
            storage,
            ByteReadRequest::new(object.clone()).with_options(options),
            "read_bhl_page",
        )
        .await?;
        if alternative != data {
            return Err(error::invalid_pages(format!(
                "different OCR payloads for aliases matching authoritative SequenceOrder: {} and {}",
                canonical.locator(),
                object.locator()
            ))
            .with_object(Some(object)));
        }
    }
    for duplicate in selection.objects.iter().skip(1) {
        report(&BhlPageReconciliation::Deduplicated {
            canonical: canonical.clone(),
            duplicate: duplicate.clone(),
        })?;
    }
    Ok(data)
}
