use std::{num::NonZeroUsize, sync::Arc};

use arrow_array::UInt64Array;
use arrow_schema::{DataType, Field, Schema};
use futures::{StreamExt, stream};
use open_arrow_access_core::{
    Accessor, AccessorExt, BytePosition, ByteReader, ErrorKind, FactoryAccessor, ReadOptions,
    ReadRequest, Result,
};
use open_arrow_access_file_format::csv::{CsvFileFormat, CsvHeader};
use open_arrow_access_layers::{SourceColumnsLayer, SourceFramer, SourceUnit};
use open_arrow_access_test_support::MemoryAccessor;

// The caller supplies coordinates from an index. This framer deliberately
// needs no positioned input and has no dependency on WARC or compression.
struct IndexedUnit(Vec<BytePosition>);
impl SourceFramer for IndexedUnit {
    fn frame(
        &self,
        mut input: Box<dyn ByteReader>,
    ) -> futures::stream::BoxStream<'static, Result<SourceUnit>> {
        let positions = self.0.clone();
        stream::once(async move {
            let mut bytes = Vec::new();
            while let Some(chunk) = input.next().await? {
                bytes.extend_from_slice(&chunk);
            }
            Ok(SourceUnit {
                bytes: bytes.into(),
                positions,
            })
        })
        .boxed()
    }
}

#[tokio::test]
async fn generic_framing_validates_rows_even_with_empty_projection() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));
    // A unit yields several batches. Too few/many positions must fail before
    // any batch is exposed, including when no output columns are selected.
    for position_count in [1, 2, 3] {
        for projection in [None, Some(Vec::new())] {
            let source = FactoryAccessor::new(
                MemoryAccessor::new(b"1\n2\n", 1),
                CsvFileFormat::new(schema.clone()).with_header(CsvHeader::Absent),
            )
            .layer(
                SourceColumnsLayer::new()
                    .with_framer(IndexedUnit(
                        (0..position_count)
                            .map(|i| BytePosition::Stored(100 + i))
                            .collect(),
                    ))
                    .with_offset("offset"),
            );
            let mut reader = source
                .open(ReadRequest::new("data.csv").with_options(ReadOptions {
                    batch_size: NonZeroUsize::MIN,
                    projection: projection.clone(),
                }))
                .await
                .unwrap();
            if position_count != 2 {
                let error = reader.next().await.unwrap_err();
                assert_eq!(error.kind(), ErrorKind::Decode);
                assert!(error.to_string().contains("data.csv"));
                assert!(reader.next().await.unwrap().is_none());
                continue;
            }
            for i in 0..2 {
                let batch = reader.next().await.unwrap().unwrap();
                assert_eq!(batch.num_rows(), 1);
                if projection.is_some() {
                    assert_eq!(batch.num_columns(), 0);
                } else {
                    let offsets = batch
                        .column_by_name("offset")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap();
                    assert_eq!(offsets.value(0), 100 + i);
                }
            }
            assert!(reader.next().await.unwrap().is_none());
        }
    }
}

struct HeaderUnits;
impl SourceFramer for HeaderUnits {
    fn frame(
        &self,
        _input: Box<dyn ByteReader>,
    ) -> futures::stream::BoxStream<'static, Result<SourceUnit>> {
        stream::iter(["id\n1\n", "id,extra\n2,3\n"].into_iter().map(|text| {
            Ok(SourceUnit {
                bytes: text.into(),
                positions: vec![BytePosition::Stored(0)],
            })
        }))
        .boxed()
    }
}

#[tokio::test]
async fn later_unit_schema_drift_is_terminal() {
    let source = FactoryAccessor::new(
        MemoryAccessor::new(b"id\n1\nid,extra\n2,3\n", 1),
        CsvFileFormat::infer(),
    )
    .layer(
        SourceColumnsLayer::new()
            .with_framer(HeaderUnits)
            .with_offset("offset"),
    );
    let mut reader = source.open(ReadRequest::new("data.csv")).await.unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Decode);
    assert!(error.to_string().contains("unit schema differs"));
    assert!(error.to_string().contains("data.csv"));
    assert!(reader.next().await.unwrap().is_none());
}
