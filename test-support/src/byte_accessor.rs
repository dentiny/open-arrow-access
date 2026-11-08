//! Shared byte-accessor contracts; backend-specific failures stay in backend tests.

use std::ops::Range;

use open_arrow_access_core::{Accessor, ByteReadRequest, ByteReader, Result};

use crate::collect_bytes;

/// Run exact single/bulk ranges, cursor preservation and independent-reader
/// ownership contracts together. The caller supplies fixture-appropriate ranges.
pub async fn assert_ranged_read_contract<A>(
    accessor: A,
    request: ByteReadRequest,
    expected: &[u8],
    ranges: &[Range<u64>],
) -> Result<()>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    let mut reader = accessor.open(request.clone()).await?;
    assert_ranges(reader.as_mut(), expected, ranges).await?;
    // Neither the single nor bulk range operations may move the sequential cursor.
    assert_reader(reader.as_mut(), expected).await?;
    drop(reader);
    assert_read_contract(accessor, request, expected).await
}

/// Check independent cursors, reader ownership, exact output, stable metadata, and EOF.
/// Takes ownership so readers must remain usable after the accessor is dropped.
pub async fn assert_read_contract<A>(
    accessor: A,
    request: ByteReadRequest,
    expected: &[u8],
) -> Result<()>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    assert!(accessor.capability().read);
    let mut first = accessor.open(request.clone()).await?;
    let mut second = accessor.open(request).await?;
    let first_metadata = first.metadata().clone();
    for reader in [&first, &second] {
        if let Some(length) = reader.metadata().content_length {
            assert_eq!(length, expected.len() as u64);
        }
    }
    // Advance one cursor before consuming the other.
    let prefix = first.next().await?;
    drop(accessor);
    assert_reader(second.as_mut(), expected).await?;
    let mut output = prefix.map_or_else(Vec::new, |chunk| {
        assert!(!chunk.is_empty());
        chunk.to_vec()
    });
    output.extend(collect_bytes(first.as_mut()).await?);
    assert_eq!(output, expected);
    assert_eq!(first.metadata().source, first_metadata.source);
    assert_eq!(
        first.metadata().content_length,
        first_metadata.content_length
    );
    assert_eq!(first.metadata().version, first_metadata.version);
    assert_reader(first.as_mut(), b"").await?;
    Ok(())
}

async fn assert_reader(reader: &mut dyn ByteReader, expected: &[u8]) -> Result<()> {
    let metadata = reader.metadata().clone();
    assert!(!reader.capability().list);
    assert_eq!(collect_bytes(reader).await?, expected);
    assert!(reader.next().await?.is_none());
    assert!(reader.next().await?.is_none());
    assert_eq!(reader.metadata().source, metadata.source);
    assert_eq!(reader.metadata().content_length, metadata.content_length);
    assert_eq!(reader.metadata().version, metadata.version);
    Ok(())
}

/// Check exact single and bulk ranges, including caller order and duplicates.
/// Call only for ranged readers; ranges must be valid in `expected`.
/// Sequential cursor checks remain possible after this helper returns.
pub async fn assert_ranges(
    reader: &mut dyn ByteReader,
    expected: &[u8],
    ranges: &[Range<u64>],
) -> Result<()> {
    assert!(reader.capability().read_with_range);
    for range in ranges {
        assert_eq!(
            reader.read_range(range.clone()).await?.as_ref(),
            &expected[range.start as usize..range.end as usize]
        );
    }
    let output = reader.read_ranges(ranges.to_vec()).await?;
    assert_eq!(output.len(), ranges.len());
    for (bytes, range) in output.iter().zip(ranges) {
        assert_eq!(
            bytes.as_ref(),
            &expected[range.start as usize..range.end as usize]
        );
    }
    assert!(reader.read_ranges(vec![]).await?.is_empty());
    Ok(())
}
