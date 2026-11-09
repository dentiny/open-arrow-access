use futures::TryStreamExt;
use open_arrow_access_core::{Accessor, ErrorKind, read_lines};
use open_arrow_access_test_support::{MemoryAccessor, fixture};

#[tokio::test]
async fn line_endings_binary_tail_and_chunk_boundaries() {
    let bytes = fixture!("bytes", "line_endings.bin").await;
    for chunk in [1, 2, 3, 16, 31, 32, 33, 63, 64, 65, 4096, 65536] {
        let input = MemoryAccessor::new(&bytes, chunk)
            .open("lines".into())
            .await
            .unwrap();
        let lines: Vec<_> = read_lines(input, Some(4097), "test_lines")
            .try_collect()
            .await
            .unwrap();
        assert_eq!(lines.len(), 4, "chunk={chunk}");
        assert_eq!(lines[0].as_ref(), b"\n");
        assert_eq!(lines[1].as_ref(), b"\r\n");
        assert_eq!(lines[2].as_ref(), &bytes[3..4100]);
        assert_eq!(lines[3].as_ref(), b"last\xff");
    }
}

#[tokio::test]
async fn line_limit_includes_terminator_across_chunks_and_errors_are_terminal() {
    let bytes = fixture!("bytes", "line_endings.bin").await;
    for chunk in [1, 31, 65536] {
        let input = MemoryAccessor::new(&bytes, chunk)
            .open("lines".into())
            .await
            .unwrap();
        let lines = read_lines(input, Some(4096), "test_lines");
        futures::pin_mut!(lines);
        assert_eq!(lines.try_next().await.unwrap().unwrap().as_ref(), b"\n");
        assert_eq!(lines.try_next().await.unwrap().unwrap().as_ref(), b"\r\n");
        let error = lines.try_next().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode);
        assert_eq!(error.operation(), "test_lines");
        assert!(lines.try_next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn empty_input_has_no_line() {
    let bytes = fixture!("bytes", "empty.bin").await;
    let input = MemoryAccessor::new(&bytes, 64)
        .open("lines".into())
        .await
        .unwrap();
    let lines: Vec<_> = read_lines(input, Some(0), "test_lines")
        .try_collect()
        .await
        .unwrap();
    assert!(lines.is_empty());
}
