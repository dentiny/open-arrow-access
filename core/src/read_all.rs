use crate::{ByteReader, Result};

const MAX_PREALLOCATION_BYTES: u64 = 64 * 1024 * 1024;

/// Collect an owned byte reader, preserving source errors and rejecting empty chunks.
///
/// A known length reserves up to 64 MiB after the first nonempty chunk, avoiding
/// repeated buffer growth for common whole-object reads. This is an allocation
/// hint only: unknown or stale lengths never truncate or reject actual input.
pub async fn read_all<R>(mut reader: Box<R>, operation: &'static str) -> Result<Vec<u8>>
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
    let mut output = Vec::new();
    while let Some(bytes) = reader.next().await? {
        if bytes.is_empty() {
            return Err(crate::error::empty_chunk(operation));
        }
        if output.is_empty() {
            output.reserve_exact(initial_capacity);
        }
        output.extend_from_slice(&bytes);
    }
    Ok(output)
}
