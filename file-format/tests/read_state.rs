use std::{
    num::NonZeroUsize,
    sync::{Arc, atomic::Ordering},
};

use arrow_array::Int64Array;
use arrow_schema::{DataType, Field, Schema};
use open_arrow_access_core::{Accessor, ErrorKind, ReadOptions, ReadRequest, RecordReader};
use open_arrow_access_file_format::{CsvAccessor, JsonAccessor};
use open_arrow_access_test_support::MemoryAccessor;

fn accessor(
    input: MemoryAccessor,
    csv: bool,
) -> Box<dyn Accessor<Request = ReadRequest, Reader = dyn RecordReader>> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    if csv {
        Box::new(CsvAccessor::new(input, schema))
    } else {
        Box::new(JsonAccessor::new(input, schema))
    }
}

fn request(batch_size: usize) -> ReadRequest {
    ReadRequest::new("lifecycle").with_options(ReadOptions {
        batch_size: NonZeroUsize::new(batch_size).unwrap(),
        ..ReadOptions::default()
    })
}

#[tokio::test]
async fn buffered_final_rows_survive_input_eof_without_repolling_the_source() {
    for (csv, data) in [(true, b"7".as_slice()), (false, br#"{"id":7}"#)] {
        let input = MemoryAccessor::new(data, 1);
        let observations = input.observations.clone();
        let mut reader = accessor(input, csv).open(request(2)).await.unwrap();
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.schema(), reader.metadata().schema);
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            7
        );
        // Finishing the partial batch required reading input EOF.
        assert_eq!(observations.reads.load(Ordering::SeqCst), data.len() + 1);
        for _ in 0..3 {
            assert!(reader.next().await.unwrap().is_none());
        }
        assert_eq!(observations.reads.load(Ordering::SeqCst), data.len() + 1);
    }
}

#[tokio::test]
async fn a_later_decode_error_is_terminal_without_replaying_a_delivered_batch() {
    for (csv, data) in [
        (true, b"7\nbad".as_slice()),
        (false, b"{\"id\":7}\n{\"id\":\"bad\"}"),
    ] {
        let input = MemoryAccessor::new(data, 1);
        let observations = input.observations.clone();
        let mut reader = accessor(input, csv).open(request(1)).await.unwrap();
        let first = reader.next().await.unwrap().unwrap();
        assert_eq!(first.num_rows(), 1);
        assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
        let reads = observations.reads.load(Ordering::SeqCst);
        for _ in 0..3 {
            assert!(reader.next().await.unwrap().is_none());
        }
        assert_eq!(observations.reads.load(Ordering::SeqCst), reads);
        assert_eq!(
            first
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            7
        );
    }
}
