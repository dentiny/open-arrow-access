#![cfg(feature = "format-xml")]

#[cfg(all(
    feature = "storage-fs",
    feature = "decompression-gzip",
    feature = "dataset-directory"
))]
#[tokio::test]
async fn xml_composes_with_fs_gzip_and_ordered_file_sets() {
    use arrow_array::Int64Array;
    use arrow_schema::DataType::Int64;
    use open_arrow_access::{
        Accessor, AccessorExt,
        dataset::{FileSetAccessor, FileSetRequest},
        decompression::{DecompressionLayer, Gzip},
        file_format::{NamespaceBinding, XmlAccessor, XmlField, XmlSchema},
        storage::FsAccessor,
    };
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/xml");
    let bytes = FsAccessor::new(root)
        .await
        .unwrap()
        .layer(DecompressionLayer::new(Gzip));
    let schema = XmlSchema::with_namespaces(
        vec![XmlField::new("id", "@m:id", Int64, false)],
        &[
            NamespaceBinding {
                prefix: "b",
                uri: "urn:catalog",
            },
            NamespaceBinding {
                prefix: "m",
                uri: "urn:meta",
            },
        ],
    )
    .unwrap();
    let xml = XmlAccessor::new(bytes, schema, "/b:catalog/b:book");
    let files = FileSetAccessor::new(xml);
    let mut reader = files
        .open(FileSetRequest::new(["catalog.xml.gz", "catalog.xml.gz"]))
        .await
        .unwrap();
    let mut ids = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.schema(), reader.metadata().schema);
        ids.extend_from_slice(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
        );
    }
    assert_eq!(ids, vec![1, 2, 3, 1, 2, 3]);
}
