use std::sync::Arc;

use arrow_array::{ArrayRef, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use futures::stream;
use open_arrow_access_core::{
    Accessor, BytePosition, ErrorKind, OrderRequirement, PositionedBatch, PositionedRecordReader,
    ReadOptions, ReadRequest, RecordBatch, RecordCapability, RecordMetadata, Result, StreamReader,
    async_trait,
};
use open_arrow_access_layers::SourceColumns;
#[cfg(feature = "schema-evolution")]
use open_arrow_access_layers::{ColumnEvolution, SchemaEvolutionLayer};

// Coordinates may come from an external index. This accessor has no byte input,
// WARC parser, or factory; the adapter only needs decoded batches with positions.
struct IndexedSource {
    positions: usize,
    drift: bool,
}

fn schema(extra: bool) -> Arc<Schema> {
    let mut fields = vec![Field::new("id", DataType::UInt64, false)];
    if extra {
        fields.push(Field::new("extra", DataType::UInt64, false));
    }
    Arc::new(Schema::new(fields))
}
fn batch(extra: bool, values: Vec<u64>) -> RecordBatch {
    let array: ArrayRef = Arc::new(UInt64Array::from(values));
    let arrays = if extra {
        vec![array.clone(), array]
    } else {
        vec![array]
    };
    RecordBatch::try_new(schema(extra), arrays).unwrap()
}
#[async_trait]
impl Accessor for IndexedSource {
    type Request = ReadRequest;
    type Reader = dyn PositionedRecordReader;
    fn capability(&self) -> RecordCapability {
        RecordCapability::sequential(OrderRequirement::Reorderable)
    }
    async fn open(&self, request: ReadRequest) -> Result<Box<dyn PositionedRecordReader>> {
        let count = self.positions;
        let first = stream::once(async move {
            PositionedBatch::try_new(
                batch(false, vec![1, 2]),
                (0..count)
                    .map(|i| BytePosition::Stored(100 + i as u64))
                    .collect(),
            )
        });
        use futures::StreamExt;
        let drift = self.drift;
        let second = stream::iter(drift.then(|| {
            PositionedBatch::try_new(batch(true, vec![3]), vec![BytePosition::Stored(102)])
        }));
        Ok(Box::new(StreamReader::records(
            first.chain(second),
            RecordMetadata {
                schema: schema(false),
                source: Some(request.object),
            },
            OrderRequirement::Reorderable,
        )))
    }
}

#[tokio::test]
async fn source_offsets_survive_projection() {
    for projection in [None, Some(Vec::new())] {
        let source = SourceColumns::new()
            .with_offset("offset")
            .wrap(IndexedSource {
                positions: 2,
                drift: false,
            });
        let mut reader = source
            .open(ReadRequest::new("indexed.csv").with_options(ReadOptions {
                projection: projection.clone(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        if projection.is_some() {
            assert_eq!(batch.num_columns(), 0);
        } else {
            let offsets = batch
                .column_by_name("offset")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            assert_eq!(offsets.values().as_ref(), [100, 101]);
        }
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn source_position_errors_retain_context_and_are_terminal() {
    let source = SourceColumns::new()
        .with_offset("offset")
        .wrap(IndexedSource {
            positions: 1,
            drift: false,
        });
    let mut reader = source.open(ReadRequest::new("indexed.csv")).await.unwrap();
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Decode);
    assert!(error.to_string().contains("indexed.csv"));
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn later_batch_schema_drift_is_terminal() {
    let source = SourceColumns::new()
        .with_offset("offset")
        .wrap(IndexedSource {
            positions: 2,
            drift: true,
        });
    let mut reader = source.open(ReadRequest::new("indexed.csv")).await.unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 2);
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Decode);
    assert!(error.to_string().contains("source batch schema differs"));
    assert!(error.to_string().contains("indexed.csv"));
    assert!(reader.next().await.unwrap().is_none());
}

#[cfg(feature = "schema-evolution")]
#[tokio::test]
async fn row_preserving_evolution_retains_coordinates_across_casts_and_generated_columns() {
    let input = IndexedSource {
        positions: 2,
        drift: false,
    }
    .open(ReadRequest::new("indexed.csv"))
    .await
    .unwrap();
    let input = SchemaEvolutionLayer::try_new(vec![
        ColumnEvolution::change_data_type("id", DataType::Utf8),
        ColumnEvolution::add_column_sql("copy", "id"),
    ])
    .unwrap()
    .wrap_positioned_reader(input)
    .unwrap();
    let mut reader = input;
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(
        batch.positions(),
        [BytePosition::Stored(100), BytePosition::Stored(101)]
    );
    assert_eq!(batch.batch().schema().field(0).data_type(), &DataType::Utf8);
    assert_eq!(
        batch.batch().column(0).to_data(),
        batch.batch().column(1).to_data()
    );
    assert_eq!(reader.metadata().schema, batch.batch().schema());
    assert!(reader.next().await.unwrap().is_none());
}
