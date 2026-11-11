use std::{num::NonZeroUsize, sync::Arc};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use open_arrow_access_core::{Accessor, AccessorExt, ReadOptions, ReadRequest, into_stream};
use open_arrow_access_file_format::parquet::ParquetAccessor;
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_test_support::MemoryAccessor;
use parquet::{
    arrow::ArrowWriter,
    basic::Compression,
    file::properties::{WriterProperties, WriterVersion},
};

#[tokio::test]
async fn compressed_pages_preserve_values_projection_and_parallel_row_groups() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("text", DataType::Utf8, true),
    ]));
    let expected = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(0..7)),
            Arc::new(StringArray::from(vec![
                Some("zero"),
                None,
                Some("two"),
                Some("three"),
                None,
                Some("five"),
                Some("six"),
            ])),
        ],
    )
    .unwrap();
    for compression in [Compression::GZIP(Default::default())] {
        for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
            for dictionary in [false, true] {
                let properties = WriterProperties::builder()
                    .set_compression(compression)
                    .set_writer_version(version)
                    .set_dictionary_enabled(dictionary)
                    .set_max_row_group_row_count(Some(3))
                    .build();
                let mut bytes = Vec::new();
                let mut writer =
                    ArrowWriter::try_new(&mut bytes, expected.schema(), Some(properties)).unwrap();
                writer.write(&expected).unwrap();
                writer.close().unwrap();
                let accessor = ParquetAccessor::new(MemoryAccessor::new(&bytes, 17).with_ranges());
                for projection in [None, Some(vec!["text".into(), "id".into()]), Some(vec![])] {
                    let projected = match &projection {
                        None => expected.clone(),
                        Some(fields) if fields.is_empty() => expected.project(&[]).unwrap(),
                        Some(_) => expected.project(&[1, 0]).unwrap(),
                    };
                    let request = ReadRequest::new("data.parquet").with_options(ReadOptions {
                        batch_size: NonZeroUsize::new(2).unwrap(),
                        projection,
                    });
                    let sequential = into_stream(accessor.open(request.clone()).await.unwrap())
                        .try_collect::<Vec<_>>()
                        .await
                        .unwrap();
                    let parallel = accessor.clone().layer(ParallelReadLayer::new(
                        NonZeroUsize::new(2).unwrap(),
                        NonZeroUsize::new(1).unwrap(),
                    ));
                    let partitioned = into_stream(parallel.open(request).await.unwrap())
                        .try_collect::<Vec<_>>()
                        .await
                        .unwrap();
                    for batches in [sequential, partitioned] {
                        let mut offset = 0;
                        for batch in batches {
                            assert!(batch.num_rows() > 0 && batch.num_rows() <= 2);
                            assert_eq!(
                                batch,
                                projected.slice(offset, batch.num_rows()),
                                "{compression:?}, {version:?}, dictionary={dictionary}"
                            );
                            offset += batch.num_rows();
                        }
                        assert_eq!(offset, expected.num_rows());
                    }
                }
            }
        }
    }
}
