use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use futures::stream;
use open_arrow_access_core::{
    Error, ErrorKind, Reader, RecordBatch, RecordMetadata, RecordReader, ScanPartition, ScanPlan,
    Schema, StreamReader,
};

#[tokio::test]
async fn sequential_plan_opens_lazily_and_stops_at_discovery_open_or_read_errors() {
    for failure in 0..3 {
        let metadata = RecordMetadata {
            schema: Arc::new(Schema::empty()),
            source: Some("manifest".into()),
        };
        let opened = Arc::new(AtomicUsize::new(0));
        let partitions = (0..3)
            .map(|index| {
                let opened = opened.clone();
                let metadata = metadata.clone();
                if index == 1 && failure == 0 {
                    return Err(Error::new(ErrorKind::Io, "discover", "fixture"));
                }
                Ok(Box::pin(async move {
                    opened.fetch_add(1, Ordering::SeqCst);
                    if index == 1 && failure == 1 {
                        return Err(Error::new(ErrorKind::Io, "open", "fixture"));
                    }
                    let item = if index == 1 {
                        Err(Error::new(ErrorKind::Io, "read", "fixture"))
                    } else {
                        Ok(RecordBatch::new_empty(metadata.schema.clone()))
                    };
                    Ok(Box::new(StreamReader::new(stream::iter([item]), metadata))
                        as Box<dyn RecordReader>)
                }) as ScanPartition)
            })
            .collect::<Vec<_>>();
        let mut reader =
            ScanPlan::from_stream(metadata.clone(), stream::iter(partitions)).into_reader();
        assert_eq!(reader.metadata().schema, metadata.schema);
        assert_eq!(reader.metadata().source, metadata.source);
        assert_eq!(opened.load(Ordering::SeqCst), 0);
        assert!(reader.next().await.unwrap().is_some());
        assert_eq!(opened.load(Ordering::SeqCst), 1);
        let error = reader.next().await.unwrap_err();
        assert_eq!(error.operation(), ["discover", "open", "read"][failure]);
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(
            opened.load(Ordering::SeqCst),
            if failure == 0 { 1 } else { 2 }
        );
    }
}
