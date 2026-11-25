use crate::bhl::{error, metadata::BhlPageSelection};
use open_arrow_access_core::{
    ByteAccessor, ByteReadOptions, ByteReadRequest, ObjectRef, Result, read_object,
};
use std::sync::{Arc, Mutex};

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
    /// No alias is emitted when canonical OCR bodies disagree.
    ConflictingPayloads {
        canonical: ObjectRef,
        conflicting: ObjectRef,
        aliases: Vec<ObjectRef>,
    },
    /// Aliases differ only in LF versus CRLF line endings. Original bytes from
    /// the canonical object are retained without normalizing the output.
    LineEndingDeduplicated {
        canonical: ObjectRef,
        duplicate: ObjectRef,
    },
    /// A listed directory contains no per-page OCR (aggregate TXT is excluded).
    EmptyShard { prefix: String },
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
) -> Result<Option<Vec<u8>>> {
    let canonical = &selection.page.object;
    let data = read_object(
        storage,
        ByteReadRequest::new(canonical.clone()).with_options(options),
        "read_bhl_page",
    )
    .await?;
    // At most the canonical body and one alternative body are retained. Verify
    // every alias before reporting deduplication or emitting the canonical row.
    let mut events = Vec::new();
    for object in selection.objects.iter().skip(1) {
        if object == canonical {
            events.push(BhlPageReconciliation::Deduplicated {
                canonical: canonical.clone(),
                duplicate: object.clone(),
            });
            continue;
        }
        let alternative = read_object(
            storage,
            ByteReadRequest::new(object.clone()).with_options(options),
            "read_bhl_page",
        )
        .await?;
        let identical = alternative == data;
        if !identical && !lf_bytes(&alternative).eq(lf_bytes(&data)) {
            report(&BhlPageReconciliation::ConflictingPayloads {
                canonical: canonical.clone(),
                conflicting: object.clone(),
                aliases: selection.objects.clone(),
            })?;
            return Ok(None);
        }
        events.push(if identical {
            BhlPageReconciliation::Deduplicated {
                canonical: canonical.clone(),
                duplicate: object.clone(),
            }
        } else {
            BhlPageReconciliation::LineEndingDeduplicated {
                canonical: canonical.clone(),
                duplicate: object.clone(),
            }
        });
    }
    for event in events {
        report(&event)?;
    }
    Ok(Some(data))
}

// Ignore only CR immediately preceding LF: lone CR, whitespace, encoding and
// all other bytes remain significant. No normalized payload allocation is needed.
fn lf_bytes(bytes: &[u8]) -> impl Iterator<Item = u8> + '_ {
    bytes.iter().enumerate().filter_map(|(index, &byte)| {
        if byte == b'\r' && bytes.get(index + 1) == Some(&b'\n') {
            None
        } else {
            Some(byte)
        }
    })
}

// Discovery and OCR partitions can finish in either order. The last participant
// checks for empty output without serializing page reads or waiting for workers.
#[derive(Default)]
pub(super) struct ScanCompletion(Mutex<CompletionState>);

#[derive(Default)]
struct CompletionState {
    pending: usize,
    discovered: bool,
    emitted: bool,
    first: Option<ObjectRef>,
}

impl ScanCompletion {
    pub(super) fn selected(&self, object: &ObjectRef) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.pending += 1;
        state.first.get_or_insert_with(|| object.clone());
    }

    pub(super) fn completed(&self, emitted: bool) -> Result<()> {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.pending -= 1;
        state.emitted |= emitted;
        state.validate()
    }

    pub(super) fn discovery_finished(&self) -> Result<()> {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.discovered = true;
        state.validate()
    }
}

impl CompletionState {
    fn validate(&self) -> Result<()> {
        if self.discovered && self.pending == 0 && !self.emitted {
            return Err(error::invalid_pages(
                "no OCR page survived authoritative SequenceOrder reconciliation; all selected pages quarantined",
            ).with_object(self.first.as_ref()));
        }
        Ok(())
    }
}
