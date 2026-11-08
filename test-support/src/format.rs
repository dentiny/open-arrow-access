//! Common assertions for single-file Arrow format accessors.
//!
//! Fixtures, parsed values and format-specific error context stay with the caller.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use open_arrow_access_core::{
    Accessor, Error, ErrorKind, ObjectRef, ReadRequest, RecordBatch, RecordMetadata, RecordReader,
    SchemaRef,
};

use crate::{MemoryAccessor, Observations};

/// One valid fixture for a sequential format with a fixed, explicit schema.
/// Expected schema and rows come from the test, independently of the accessor.
/// The full schema must contain a field to exercise invalid projections.
pub struct FormatCase<'a> {
    pub bytes: &'a [u8],
    /// The selected projection is returned for format-specific value assertions.
    pub request: ReadRequest,
    pub schema: SchemaRef,
    pub rows: usize,
    /// Reachable input positions for source-error and empty-chunk injection.
    pub fault_offsets: &'a [usize],
}

/// Run projection, validation-before-I/O and source-failure contracts together.
/// Also reads full and zero-column output, retaining the request's byte options.
/// Inferred schemas and range-based inputs use the individual helpers instead:
/// their validation timing and fault injection differ from this contract.
pub async fn assert_fixed_schema_contract<A>(
    make_format: impl Fn(MemoryAccessor) -> A,
    case: FormatCase<'_>,
) -> Vec<RecordBatch>
where
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader>,
{
    let input = MemoryAccessor::new(case.bytes, 1);
    let observed = input.observations.clone();
    let accessor = make_format(input);
    assert_invalid_projection_before_open(&accessor, case.request.clone(), &case.schema, &observed)
        .await;
    let selected =
        assert_projection(&accessor, case.request.clone(), &case.schema, case.rows).await;
    for projection in [None, Some(vec![])] {
        if case.request.options.projection == projection {
            continue;
        }
        let mut request = case.request.clone();
        request.options.projection = projection;
        assert_projection(&accessor, request, &case.schema, case.rows).await;
    }
    // Fault scenarios exercise full decoding and can deliver one row at a time,
    // independently of the happy-path projection and batch target.
    let mut fault_request = case.request;
    fault_request.options.projection = None;
    fault_request.options.batch_size = std::num::NonZeroUsize::MIN;
    assert_error_contract(make_format, case.bytes, fault_request, case.fault_offsets).await;
    selected
}

/// After an observed EOF or error, require stable metadata and no upstream polls.
pub async fn assert_finished(reader: &mut dyn RecordReader, reads: &AtomicUsize) {
    crate::reader::assert_finished(reader, reads, metadata_snapshot).await;
}

/// Require the next call to fail, then verify terminal behavior. Return the error
/// so the caller can check format-specific operation names and parser causes.
pub async fn assert_terminal_error(
    reader: &mut dyn RecordReader,
    reads: &AtomicUsize,
    kind: ErrorKind,
) -> Error {
    crate::reader::assert_terminal_error(reader, reads, kind, metadata_snapshot).await
}

fn metadata_snapshot(metadata: &RecordMetadata) -> (SchemaRef, Option<ObjectRef>) {
    (metadata.schema.clone(), metadata.source.clone())
}

/// Collect a successful single-file read, checking source identity, schema
/// stability, nonempty batches, the requested row target and terminal EOF.
pub async fn collect_batches<A>(accessor: &A, request: ReadRequest) -> Vec<RecordBatch>
where
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader> + ?Sized,
{
    let reader = accessor.open(request.clone()).await.unwrap();
    collect_reader(reader, &request).await
}

async fn collect_reader(
    mut reader: Box<dyn RecordReader>,
    request: &ReadRequest,
) -> Vec<RecordBatch> {
    assert_eq!(reader.metadata().source.as_ref(), Some(&request.object));
    let schema = reader.metadata().schema.clone();
    let capability = reader.capability();
    assert!(capability.read);
    assert!(!capability.read_with_range && !capability.list);
    let mut batches = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        assert!(batch.num_rows() > 0);
        assert!(batch.num_rows() <= request.options.batch_size.get());
        assert_eq!(batch.schema(), schema);
        assert_eq!(reader.metadata().schema, schema);
        assert_eq!(reader.metadata().source.as_ref(), Some(&request.object));
        batches.push(batch);
    }
    assert!(reader.next().await.unwrap().is_none());
    batches
}

/// Check caller column order, field/schema metadata and total row count,
/// including zero-column projections. Return batches for format-specific values.
/// `schema` is the caller's expected full schema, independent of the accessor.
pub async fn assert_projection<A>(
    accessor: &A,
    request: ReadRequest,
    schema: &SchemaRef,
    rows: usize,
) -> Vec<RecordBatch>
where
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader> + ?Sized,
{
    let expected = match &request.options.projection {
        Some(names) => Arc::new(
            schema
                .project(
                    &names
                        .iter()
                        .map(|name| schema.index_of(name).unwrap())
                        .collect::<Vec<_>>(),
                )
                .unwrap(),
        ),
        None => schema.clone(),
    };
    let reader = accessor.open(request.clone()).await.unwrap();
    assert_eq!(reader.metadata().schema, expected);
    let batches = collect_reader(reader, &request).await;
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        rows
    );
    for batch in &batches {
        assert_eq!(batch.schema(), expected);
        assert_eq!(batch.num_columns(), expected.fields().len());
    }
    batches
}

/// Reject missing and duplicate columns without opening bytes. Only use this
/// for fixed/explicit schemas; inferred schemas may need I/O before validation.
pub async fn assert_invalid_projection_before_open<A>(
    accessor: &A,
    request: ReadRequest,
    schema: &SchemaRef,
    observed: &Observations,
) where
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader> + ?Sized,
{
    let first = schema
        .fields()
        .first()
        .expect("contract requires a field")
        .name();
    let mut missing = "__missing__".to_owned();
    while schema.index_of(&missing).is_ok() {
        missing.push('_');
    }
    let opens = observed.requests.lock().unwrap().len();
    let reads = observed.reads.load(Ordering::SeqCst);
    for names in [vec![missing], vec![first.clone(), first.clone()]] {
        let mut request = request.clone();
        request.options.projection = Some(names);
        let error = accessor
            .open(request)
            .await
            .err()
            .expect("accepted invalid projection");
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.operation(), "project");
        assert_eq!(observed.requests.lock().unwrap().len(), opens);
        assert_eq!(observed.reads.load(Ordering::SeqCst), reads);
    }
}

/// Exercise shared input failures with Arrow metadata stability checks.
/// See [`crate::reader::assert_error_contract`] for native record readers.
pub async fn assert_error_contract<A>(
    make_format: impl Fn(MemoryAccessor) -> A,
    bytes: &[u8],
    request: ReadRequest,
    offsets: &[usize],
) where
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader>,
{
    crate::reader::assert_error_contract(
        make_format,
        bytes,
        request,
        offsets,
        metadata_snapshot,
        |_, _| {},
    )
    .await;
}
