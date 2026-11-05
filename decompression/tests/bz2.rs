#![cfg(feature = "bz2")]

mod common;

use std::{io::Write, num::NonZeroUsize};

use bzip2::{Compression, write::BzEncoder};
use open_arrow_access_core::{Accessor, AccessorExt, ErrorKind, Result};
use open_arrow_access_decompression::{Bzip2, DecompressionLayer};

use common::{MemoryAccessor, collect};

fn compress(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = BzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

#[tokio::test]
async fn concatenated_members_preserve_order_with_fragmented_input_and_bounded_output() -> Result<()>
{
    let mut bytes = compress(b"first\n");
    bytes.extend(compress(b""));
    bytes.extend(compress("第二行\n".as_bytes()));
    for chunk_size in [1, 7, bytes.len()] {
        let accessor = MemoryAccessor::new(&bytes, chunk_size)
            .layer(DecompressionLayer::new(Bzip2).with_chunk_size(NonZeroUsize::new(3).unwrap()));
        let mut reader = accessor.open("records.bz2".into()).await?;
        let mut output = Vec::new();
        while let Some(chunk) = reader.next().await? {
            assert!(!chunk.is_empty() && chunk.len() <= 3);
            output.extend_from_slice(&chunk);
        }
        assert_eq!(output, "first\n第二行\n".as_bytes());
        assert!(reader.next().await?.is_none());
    }
    let accessor = MemoryAccessor::new(&compress(b""), 1).layer(DecompressionLayer::new(Bzip2));
    let mut reader = accessor.open("empty.bz2".into()).await?;
    assert!(reader.next().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn invalid_truncated_checksum_and_trailing_input_fail_terminally() -> Result<()> {
    let valid = compress(b"payload");
    let mut block_crc = valid.clone();
    block_crc[10] ^= 1;
    let mut stream_crc = valid.clone();
    let crc_byte = stream_crc.len() - 2;
    stream_crc[crc_byte] ^= 0x80;
    let mut trailing = valid.clone();
    trailing.extend_from_slice(b"trailing junk");
    let mut truncated_member = valid.clone();
    truncated_member.extend_from_slice(&valid[..valid.len() - 1]);
    for bytes in [
        vec![],
        b"not bzip2".to_vec(),
        valid[..3].to_vec(),
        valid[..valid.len() - 1].to_vec(),
        block_crc,
        stream_crc,
        trailing,
        truncated_member,
    ] {
        let input = MemoryAccessor::new(&bytes, 1);
        let mut reader = input
            .layer(DecompressionLayer::new(Bzip2))
            .open("bad.bz2".into())
            .await
            .unwrap();
        assert_eq!(
            collect(reader.as_mut()).await.unwrap_err().kind(),
            ErrorKind::Decode
        );
        assert!(reader.next().await?.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn source_errors_are_preserved() -> Result<()> {
    let mut input = MemoryAccessor::new(&compress(b"payload"), 2);
    input.error = Some(ErrorKind::PermissionDenied);
    let accessor = input.layer(DecompressionLayer::new(Bzip2));
    let mut reader = accessor.open("source.bz2".into()).await?;
    assert_eq!(
        collect(reader.as_mut()).await.unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
    assert!(reader.next().await?.is_none());
    Ok(())
}
