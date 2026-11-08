use arrow_array::StringArray;
use arrow_schema::{DataType, Field};
use open_arrow_access_core::{
    Accessor, Capability, Error, ErrorKind, ReadRequest, RecordBatch, RecordMetadata, RecordReader,
    Result, Schema, StreamReader, async_trait,
};
use open_arrow_access_test_support::MemoryAccessor;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub(super) fn manifest(name: &str, chunk_size: usize) -> MemoryAccessor {
    let bytes = std::fs::read(open_arrow_access_test_support::fixture_path!(
        "commoncrawl",
        name
    ))
    .unwrap();
    MemoryAccessor::new(&bytes, chunk_size)
}

pub(super) fn entry(segment: &str, name: &str) -> String {
    format!("crawl-data/CC-MAIN-2022-40/segments/{segment}/warc/{name}.warc.gz")
}

#[derive(Clone, Default)]
pub(super) struct Records {
    pub(super) opens: Arc<Mutex<Vec<ReadRequest>>>,
}

#[async_trait]
impl Accessor for Records {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }
    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        self.opens.lock().unwrap().push(request.clone());
        if request.object.locator().ends_with("bad.warc.gz") {
            return Err(Error::new(ErrorKind::Io, "fixture_records", "input failed"));
        }
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("path", DataType::Utf8, false)],
            HashMap::from([("fixture".into(), "provider".into())]),
        ));
        let mut batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![request.object.locator()]))],
        )
        .unwrap();
        if let Some(columns) = &request.options.projection {
            let projection = columns
                .iter()
                .map(|name| batch.schema().index_of(name).unwrap())
                .collect::<Vec<_>>();
            batch = batch.project(&projection).unwrap();
        }
        let metadata = RecordMetadata {
            schema: batch.schema(),
            source: Some(request.object),
        };
        Ok(Box::new(StreamReader::new(
            futures::stream::iter([Ok(batch)]),
            metadata,
        )))
    }
}

pub(super) async fn paths(reader: Box<dyn RecordReader>) -> Vec<String> {
    use futures::TryStreamExt;
    open_arrow_access_core::into_stream(reader)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|value| value.unwrap().to_owned())
                .collect::<Vec<_>>()
        })
        .collect()
}
