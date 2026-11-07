use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    vec::IntoIter,
};

use futures::io::{AsyncBufReadExt, AsyncReadExt};
use open_arrow_access_core::{
    ByteMetadata, ByteReader, Bytes, Capability, Error, ErrorKind, Reader, Result, async_trait,
    into_async_read,
};

#[derive(Default)]
struct Observations {
    reads: AtomicUsize,
    dropped: AtomicBool,
}

// Deliberately does not fuse errors or EOF: the adapter must stop polling it.
struct Input {
    chunks: IntoIter<Result<Bytes>>,
    metadata: ByteMetadata,
    observed: Arc<Observations>,
}

impl Input {
    fn new(chunks: Vec<Result<Bytes>>) -> (Box<Self>, Arc<Observations>) {
        let observed = Arc::new(Observations::default());
        (
            Box::new(Self {
                chunks: chunks.into_iter(),
                metadata: ByteMetadata::default(),
                observed: observed.clone(),
            }),
            observed,
        )
    }
}

impl Drop for Input {
    fn drop(&mut self) {
        self.observed.dropped.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl Reader for Input {
    type Item = Bytes;
    type Metadata = ByteMetadata;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    fn metadata(&self) -> &ByteMetadata {
        &self.metadata
    }

    async fn next(&mut self) -> Result<Option<Bytes>> {
        self.observed.reads.fetch_add(1, Ordering::SeqCst);
        self.chunks.next().transpose()
    }
}

#[async_trait]
impl ByteReader for Input {}

#[tokio::test]
async fn buffered_and_partial_reads_preserve_chunks_without_read_ahead() {
    let first = Bytes::from_static(b"ab");
    let (input, observed) = Input::new(vec![
        Ok(first.clone()),
        Ok(Bytes::from_static(b"c\0")),
        Ok(Bytes::from_static(b"\xffdef")),
    ]);
    let mut reader = into_async_read(input, "test_input");
    assert_eq!(observed.reads.load(Ordering::SeqCst), 0);

    let buffered = reader.fill_buf().await.unwrap();
    assert_eq!(buffered, b"ab");
    assert_eq!(buffered.as_ptr(), first.as_ptr());
    reader.consume_unpin(1);
    let mut byte = [0];
    reader.read_exact(&mut byte).await.unwrap();
    assert_eq!(&byte, b"b");
    assert_eq!(observed.reads.load(Ordering::SeqCst), 1);

    let mut rest = Vec::new();
    reader.read_to_end(&mut rest).await.unwrap();
    assert_eq!(rest, b"c\0\xffdef");
    assert_eq!(observed.reads.load(Ordering::SeqCst), 4);
    assert!(reader.fill_buf().await.unwrap().is_empty());
    assert_eq!(reader.read(&mut byte).await.unwrap(), 0);
    assert_eq!(observed.reads.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn source_errors_preserve_categories_and_causes_and_are_terminal() {
    for kind in [
        ErrorKind::InvalidInput,
        ErrorKind::Unsupported,
        ErrorKind::NotFound,
        ErrorKind::PermissionDenied,
        ErrorKind::Io,
        ErrorKind::Decode,
    ] {
        let error = Error::new(kind, "source_read", "injected source failure").with_source(
            io::Error::new(io::ErrorKind::ConnectionReset, "original I/O cause"),
        );
        let (input, observed) = Input::new(vec![
            Ok(Bytes::from_static(b"prefix")),
            Err(error),
            Ok(Bytes::from_static(b"must not be read")),
        ]);
        let input: Box<dyn ByteReader> = input;
        let mut reader = into_async_read(input, "adapter_input");
        let mut output = Vec::new();
        let error = reader.read_to_end(&mut output).await.unwrap_err();
        assert_eq!(output, b"prefix");
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(ErrorKind::from_source(&error), Some(kind));
        let source = error.get_ref().unwrap().downcast_ref::<Error>().unwrap();
        assert_eq!(source.operation(), "source_read");
        let cause = std::error::Error::source(source)
            .unwrap()
            .downcast_ref::<io::Error>()
            .unwrap();
        assert_eq!(cause.kind(), io::ErrorKind::ConnectionReset);
        assert_eq!(cause.to_string(), "original I/O cause");
        assert_eq!(reader.read(&mut [0]).await.unwrap(), 0);
        assert!(reader.fill_buf().await.unwrap().is_empty());
        assert_eq!(observed.reads.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn empty_chunks_are_terminal_io_errors_with_caller_context() {
    for prefix in [None, Some(Bytes::from_static(b"valid"))] {
        let mut chunks = Vec::new();
        if let Some(bytes) = &prefix {
            chunks.push(Ok(bytes.clone()));
        }
        chunks.push(Ok(Bytes::new()));
        chunks.push(Ok(Bytes::from_static(b"must not be read")));
        let (input, observed) = Input::new(chunks);
        let mut reader = into_async_read(input, "parser_input");
        let mut output = Vec::new();
        let error = reader.read_to_end(&mut output).await.unwrap_err();
        assert_eq!(output.as_slice(), prefix.as_deref().unwrap_or_default());
        let source = error.get_ref().unwrap().downcast_ref::<Error>().unwrap();
        assert_eq!(source.kind(), ErrorKind::Io);
        assert_eq!(source.operation(), "parser_input");
        assert!(source.to_string().contains("empty chunk"));
        assert_eq!(reader.read(&mut [0]).await.unwrap(), 0);
        assert!(reader.fill_buf().await.unwrap().is_empty());
        assert_eq!(
            observed.reads.load(Ordering::SeqCst),
            if prefix.is_some() { 2 } else { 1 }
        );
    }
}

#[tokio::test]
async fn dropping_the_adapter_releases_unread_or_buffered_input() {
    for consume in [false, true] {
        let (input, observed) = Input::new(vec![Ok(Bytes::from_static(b"unread"))]);
        let mut reader = into_async_read(input, "test_input");
        if consume {
            assert_eq!(reader.fill_buf().await.unwrap(), b"unread");
        }
        assert!(!observed.dropped.load(Ordering::SeqCst));
        drop(reader);
        assert!(observed.dropped.load(Ordering::SeqCst));
        assert_eq!(observed.reads.load(Ordering::SeqCst), usize::from(consume));
    }
}
