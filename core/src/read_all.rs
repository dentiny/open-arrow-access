use crate::{ByteReader, Result};

const MAX_PREALLOCATION_BYTES: u64 = 64 * 1024 * 1024;

/// Collect an owned byte reader, preserving source errors and rejecting empty chunks.
///
/// A known length reserves up to 64 MiB after the first nonempty chunk, avoiding
/// repeated buffer growth for common whole-object reads. This is an allocation
/// hint only: unknown or stale lengths never truncate or reject actual input.
pub async fn read_all<R>(reader: Box<R>, operation: &'static str) -> Result<Vec<u8>>
where
    R: ByteReader + ?Sized,
{
    collect(reader, operation, None).await
}

/// Collect at most `max_bytes`, checking actual chunks before appending them.
///
/// Unknown/stale length hints do not bypass the limit. Exceeding it returns
/// `ResourceExhausted` and drops the reader without polling further input.
/// The limit bounds collected bytes, not upstream chunk or decoder allocations.
/// A zero limit accepts only an empty input.
pub async fn read_all_limited<R>(
    reader: Box<R>,
    operation: &'static str,
    max_bytes: usize,
) -> Result<Vec<u8>>
where
    R: ByteReader + ?Sized,
{
    collect(reader, operation, Some(max_bytes)).await
}

async fn collect<R>(
    mut reader: Box<R>,
    operation: &'static str,
    max_bytes: Option<usize>,
) -> Result<Vec<u8>>
where
    R: ByteReader + ?Sized,
{
    // Do not let an inaccurate/untrusted length request an unbounded allocation.
    // Delay reservation so empty inputs and initial failures allocate nothing.
    let initial_capacity = reader
        .metadata()
        .content_length
        .unwrap_or(0)
        .min(MAX_PREALLOCATION_BYTES) as usize;
    let initial_capacity = initial_capacity.min(max_bytes.unwrap_or(usize::MAX));
    let mut output = Vec::new();
    while let Some(bytes) = reader.next().await? {
        if bytes.is_empty() {
            return Err(crate::error::empty_chunk(operation));
        }
        crate::validation::validate_collected_bytes(
            output.len(),
            bytes.len(),
            max_bytes,
            operation,
        )?;
        if output.is_empty() {
            output.reserve_exact(initial_capacity);
        }
        output.extend_from_slice(&bytes);
    }
    Ok(output)
}
