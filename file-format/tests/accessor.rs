use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex, atomic::Ordering},
};

use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteReadOptions, ByteReader, Capability, Error, ErrorKind, ReadOptions, ReadRequest,
    RecordMetadata, RecordReader, Result, Schema, StreamReader, async_trait,
};
use open_arrow_access_file_format::{FileAccessor, FileFormat};
use open_arrow_access_test_support::MemoryAccessor;

// An external format can implement the public protocol with all built-ins disabled.
struct ProbeFormat {
    events: Arc<Mutex<Vec<&'static str>>>,
    fail_prepare: bool,
    fail_open: bool,
}

#[async_trait]
impl FileFormat for ProbeFormat {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    type Prepared = usize;

    fn prepare(&self, request: &ReadRequest) -> Result<usize> {
        let options = &request.options;
        self.events.lock().unwrap().push("prepare");
        if self.fail_prepare {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "prepare_probe",
                "invalid format",
            ));
        }
        Ok(options.batch_size.get())
    }

    async fn create_reader(
        &self,
        _input: Box<dyn ByteReader>,
        request: ReadRequest,
        prepared: usize,
    ) -> Result<Box<dyn RecordReader>> {
        self.events.lock().unwrap().push("decode");
        assert_eq!(prepared, request.options.batch_size.get());
        if self.fail_open {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "open_probe",
                "unsupported input",
            ));
        }
        Ok(Box::new(StreamReader::new(
            stream::empty(),
            RecordMetadata {
                schema: Arc::new(Schema::empty()),
                source: Some(request.object),
            },
        )))
    }
}

#[tokio::test]
async fn external_format_prepares_each_open_and_receives_request_options() {
    let input = MemoryAccessor::new(b"", 1).with_ranges().with_listing();
    let observed = input.observations.clone();
    let events = Arc::new(Mutex::new(Vec::new()));
    let accessor = FileAccessor::from_format(
        input,
        ProbeFormat {
            events: events.clone(),
            fail_prepare: false,
            fail_open: false,
        },
    );
    assert_eq!(
        accessor.capability(),
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    );
    assert!(events.lock().unwrap().is_empty());
    assert!(observed.requests.lock().unwrap().is_empty());
    let byte_options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        io_chunk_size: NonZeroUsize::new(17),
    };
    for (batch_size, object) in [(2, "first"), (7, "second")] {
        let mut reader = accessor
            .open(
                ReadRequest::new(object)
                    .with_options(ReadOptions {
                        batch_size: NonZeroUsize::new(batch_size).unwrap(),
                        ..ReadOptions::default()
                    })
                    .with_byte_options(byte_options),
            )
            .await
            .unwrap();
        assert_eq!(reader.metadata().source.as_ref().unwrap().locator(), object);
        assert!(reader.next().await.unwrap().is_none());
    }
    assert_eq!(
        *events.lock().unwrap(),
        ["prepare", "decode", "prepare", "decode"]
    );
    let requests = observed.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].object.locator(), "first");
    assert_eq!(requests[1].object.locator(), "second");
    assert!(
        requests
            .iter()
            .all(|request| request.options == byte_options)
    );
    assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn preparation_errors_prevent_storage_io_and_decode() {
    let input = MemoryAccessor::new(b"", 1);
    let observed = input.observations.clone();
    let events = Arc::new(Mutex::new(Vec::new()));
    let accessor = FileAccessor::from_format(
        input,
        ProbeFormat {
            events: events.clone(),
            fail_prepare: true,
            fail_open: false,
        },
    );
    let error = accessor.open(ReadRequest::new("file")).await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(error.operation(), "prepare_probe");
    assert_eq!(*events.lock().unwrap(), ["prepare"]);
    assert!(observed.requests.lock().unwrap().is_empty());
    assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn format_open_errors_keep_their_category_and_context() {
    let input = MemoryAccessor::new(b"", 1);
    let observed = input.observations.clone();
    let events = Arc::new(Mutex::new(Vec::new()));
    let accessor = FileAccessor::from_format(
        input,
        ProbeFormat {
            events: events.clone(),
            fail_prepare: false,
            fail_open: true,
        },
    );
    let error = accessor.open(ReadRequest::new("file")).await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert_eq!(error.operation(), "open_probe");
    assert_eq!(*events.lock().unwrap(), ["prepare", "decode"]);
    assert_eq!(observed.requests.lock().unwrap().len(), 1);
}
