use super::{NamespaceBinding, XmlAccessor, XmlField, XmlLimits, XmlSchema};
use arrow_array::{
    Array,
    cast::AsArray,
    types::{Float64Type, Int64Type},
};
use arrow_schema::DataType::{Int64, Utf8};
use open_arrow_access_core::{Accessor, ErrorKind, ReadOptions, ReadRequest};
use open_arrow_access_test_support::MemoryAccessor;
use std::num::NonZeroUsize;

const CATALOG_PATH: &str = "/c:catalog/c:product";

fn catalog_schema() -> XmlSchema {
    XmlSchema::with_namespaces(
        vec![
            XmlField::new("name", "c:name", Utf8, false),
            XmlField::new("price", "c:price", Utf8, false),
            XmlField::new("category", "c:category", Utf8, true),
        ],
        &[NamespaceBinding {
            prefix: "c",
            uri: "urn:open-arrow-access:catalog",
        }],
    )
    .unwrap()
}

fn warehouse_schema() -> XmlSchema {
    XmlSchema::new(vec![XmlField::new("warehouse", "@id", Int64, false)]).unwrap()
}

fn options(projection: Option<Vec<&str>>) -> ReadOptions {
    ReadOptions {
        batch_size: NonZeroUsize::new(2).unwrap(),
        projection: projection.map(|names| names.into_iter().map(String::from).collect()),
    }
}

#[tokio::test]
async fn fragmented_catalog_preserves_namespaces_values_schema_and_order() {
    let input = open_arrow_access_test_support::fixture!("xml", "catalog.xml").await;
    let bytes = MemoryAccessor::new(&input, 1);
    let accessor = XmlAccessor::new(bytes, CATALOG_PATH).with_schema(catalog_schema());
    let mut reader = accessor
        .open(ReadRequest::new("catalog.xml").with_options(options(None)))
        .await
        .unwrap();
    assert_eq!(reader.metadata().schema, catalog_schema().arrow_schema());
    let mut names = Vec::new();
    let mut sizes = Vec::new();
    let mut categories = Vec::new();
    let mut prices = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.schema(), reader.metadata().schema);
        sizes.push(batch.num_rows());
        names.extend(
            batch
                .column(0)
                .as_string::<i32>()
                .iter()
                .map(|s| s.unwrap().to_owned()),
        );
        categories.extend(
            batch
                .column(2)
                .as_string::<i32>()
                .iter()
                .map(|s| s.map(String::from)),
        );
        prices.extend(
            batch
                .column(1)
                .as_string::<i32>()
                .iter()
                .map(|s| s.unwrap().to_owned()),
        );
    }
    assert_eq!(sizes, [2, 2, 1]);
    assert_eq!(prices, ["12.50", "39.95", "8.00", "24.50", "45.00"]);
    assert_eq!(
        names,
        [
            "Notebook & Pen",
            "Desk Lamp",
            "Café Mug",
            "USB Hub",
            "Travel Bag",
        ]
    );
    assert_eq!(categories[0].as_deref(), Some("stationery"));
    assert!(categories[1..].iter().all(Option::is_none));
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn inventory_preserves_nested_device_lists() {
    let input = open_arrow_access_test_support::fixture!("xml", "inventory.xml").await;
    let schema = XmlSchema::new(vec![
        XmlField::new("warehouse", "@id", Int64, false),
        XmlField::list(
            "racks",
            ".",
            false,
            XmlField::structure(
                "item",
                "rack",
                false,
                vec![
                    XmlField::new("capacity", "@capacity", Int64, false),
                    XmlField::list(
                        "devices",
                        ".",
                        false,
                        XmlField::structure(
                            "item",
                            "device",
                            false,
                            vec![
                                XmlField::new("id", "@id", Utf8, false),
                                XmlField::new("kind", "@kind", Utf8, false),
                            ],
                        ),
                    ),
                ],
            ),
        ),
    ])
    .unwrap();
    let accessor = XmlAccessor::new(MemoryAccessor::new(&input, 1), "/inventory/warehouse")
        .with_schema(schema);
    let mut reader = accessor
        .open(ReadRequest::new("inventory.xml").with_options(ReadOptions {
            batch_size: NonZeroUsize::MIN,
            projection: None,
        }))
        .await
        .unwrap();
    for warehouse in 0..2 {
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch.column(0).as_primitive::<Int64Type>().value(0),
            warehouse
        );
        let racks = batch.column(1).as_list::<i32>();
        assert_eq!(racks.value_offsets(), &[0, 2]);
        let racks = racks.values().as_struct();
        assert_eq!(
            racks
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .as_ref(),
            &[16; 2]
        );
        let devices = racks.column(1).as_list::<i32>();
        assert_eq!(devices.value_offsets(), &[0, 2, 4]);
        let devices = devices.values().as_struct();
        assert_eq!(devices.len(), 4);
        assert_eq!(devices.column(1).as_string::<i32>().value(0), "gpu");
        assert_eq!(devices.column(1).as_string::<i32>().value(1), "network");
        let ids = devices.column(0).as_string::<i32>();
        assert_eq!(
            ids.value(0),
            if warehouse == 0 {
                "north-gpu-1"
            } else {
                "south-gpu-1"
            }
        );
        assert_eq!(
            ids.value(3),
            if warehouse == 0 {
                "north-nic-2"
            } else {
                "south-nic-2"
            }
        );
    }
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn catalog_projection_skips_conversion_and_counts_zero_columns() {
    let input = open_arrow_access_test_support::fixture!("xml", "catalog.xml").await;
    let mut fields = catalog_schema().fields().to_vec();
    fields.push(XmlField::new("unused_number", "c:price", Int64, false));
    let schema = XmlSchema::with_namespaces(
        fields,
        &[NamespaceBinding {
            prefix: "c",
            uri: "urn:open-arrow-access:catalog",
        }],
    )
    .unwrap();
    let accessor =
        XmlAccessor::new(MemoryAccessor::new(&input, 3), CATALOG_PATH).with_schema(schema);
    let mut reader = accessor
        .open(ReadRequest::new("catalog.xml").with_options(options(Some(vec!["category", "name"]))))
        .await
        .unwrap();
    assert_eq!(reader.metadata().schema.field(0).name(), "category");
    let first = reader.next().await.unwrap().unwrap();
    assert_eq!(first.column(0).as_string::<i32>().value(0), "stationery");
    assert_eq!(first.column(1).as_string::<i32>().value(1), "Desk Lamp");
    let mut reader = accessor
        .open(ReadRequest::new("catalog.xml").with_options(options(Some(vec![]))))
        .await
        .unwrap();
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.num_columns(), 0);
        rows += batch.num_rows();
    }
    assert_eq!(rows, 5);
}

#[tokio::test]
async fn invalid_configuration_fails_before_opening_input() {
    let input = open_arrow_access_test_support::fixture!("xml", "catalog.xml").await;
    let bytes = MemoryAccessor::new(&input, 1);
    let observations = bytes.observations.clone();
    for (path, limits) in [
        ("relative/path", XmlLimits::default()),
        ("/unbound:root", XmlLimits::default()),
        (
            "/root",
            XmlLimits {
                max_record_bytes: 50,
                max_batch_bytes: 49,
                max_depth: 1,
            },
        ),
    ] {
        let accessor = XmlAccessor::new(bytes.clone(), path)
            .with_schema(catalog_schema())
            .with_limits(limits);
        assert_eq!(
            accessor
                .open(ReadRequest::new("input"))
                .await
                .err()
                .unwrap()
                .kind(),
            ErrorKind::InvalidInput
        );
    }
    let accessor = XmlAccessor::new(bytes, CATALOG_PATH).with_schema(catalog_schema());
    for names in [vec!["missing"], vec!["name", "name"]] {
        assert_eq!(
            accessor
                .open(ReadRequest::new("input").with_options(options(Some(names))))
                .await
                .err()
                .unwrap()
                .kind(),
            ErrorKind::InvalidInput
        );
    }
    assert!(observations.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn malformed_xml_and_limits_are_terminal() {
    for name in [
        "bad_integer.xml",
        "bad_truncated.xml",
        "bad_entity.xml",
        "bad_dtd.xml",
    ] {
        let input = open_arrow_access_test_support::fixture!("xml", name).await;
        let accessor = XmlAccessor::new(MemoryAccessor::new(&input, 1), "/inventory/warehouse")
            .with_schema(warehouse_schema());
        let mut reader = accessor.open(ReadRequest::new(name)).await.unwrap();
        let error = reader.next().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode, "{name}");
        assert!(std::error::Error::source(&error).is_some());
        assert!(reader.next().await.unwrap().is_none());
    }
    let input = open_arrow_access_test_support::fixture!("xml", "inventory.xml").await;
    for (limits, kind) in [
        (
            XmlLimits {
                max_record_bytes: 32,
                max_batch_bytes: 64,
                max_depth: 128,
            },
            "EventBytes",
        ),
        (
            XmlLimits {
                max_record_bytes: 4096,
                max_batch_bytes: 4096,
                max_depth: 2,
            },
            "Depth",
        ),
    ] {
        let accessor = XmlAccessor::new(MemoryAccessor::new(&input, 1), "/inventory/warehouse")
            .with_schema(warehouse_schema())
            .with_limits(limits);
        let mut reader = accessor
            .open(ReadRequest::new("inventory.xml"))
            .await
            .unwrap();
        let error = reader.next().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode);
        assert!(error.to_string().contains(kind));
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn inventory_batch_byte_limits_preserve_lookahead_order() {
    let input = open_arrow_access_test_support::fixture!("xml", "inventory.xml").await;
    let accessor = XmlAccessor::new(MemoryAccessor::new(&input, 1), "/inventory/warehouse")
        .with_schema(warehouse_schema())
        .with_limits(XmlLimits {
            max_record_bytes: 512,
            max_batch_bytes: 512,
            max_depth: 128,
        });
    let mut reader = accessor
        .open(ReadRequest::new("inventory.xml"))
        .await
        .unwrap();
    for warehouse in 0..2 {
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch.column(0).as_primitive::<Int64Type>().value(0),
            warehouse
        );
    }
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn path_only_infers_inventory_and_replays_bounded_sample() {
    use std::sync::atomic::Ordering;
    let input = open_arrow_access_test_support::fixture!("xml", "inventory.xml").await;
    let bytes = MemoryAccessor::new(&input, 1);
    let observations = bytes.observations.clone();
    let accessor = XmlAccessor::new(bytes, "/inventory/warehouse").with_inference_options(
        super::XmlInferenceOptions {
            max_records: 1,
            max_bytes: 4096,
        },
    );
    let mut reader = accessor
        .open(ReadRequest::new("inventory.xml").with_options(ReadOptions {
            batch_size: NonZeroUsize::MIN,
            projection: None,
        }))
        .await
        .unwrap();
    // One record was sampled without consuming the whole document.
    assert!(observations.reads.load(Ordering::Relaxed) < input.len());
    assert_eq!(
        reader.metadata().source.as_ref().unwrap().locator(),
        "inventory.xml"
    );
    let schema = reader.metadata().schema.clone();
    assert_eq!(schema.field_with_name("@id").unwrap().data_type(), &Int64);
    for warehouse in 0..2 {
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.schema(), schema);
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column_by_name("@id")
                .unwrap()
                .as_primitive::<Int64Type>()
                .value(0),
            warehouse
        );
        let racks = batch.column_by_name("rack").unwrap().as_list::<i32>();
        assert_eq!(racks.value_offsets(), &[0, 2]);
        let racks = racks.values().as_struct();
        let devices = racks.column_by_name("device").unwrap().as_list::<i32>();
        assert_eq!(devices.value_offsets(), &[0, 2, 4]);
        let devices = devices.values().as_struct();
        assert_eq!(
            devices
                .column_by_name("@id")
                .unwrap()
                .as_string::<i32>()
                .value(0),
            if warehouse == 0 {
                "north-gpu-1"
            } else {
                "south-gpu-1"
            }
        );
    }
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
    // Project after inference, preserving caller order and zero-column row counts.
    for names in [vec!["@owner", "@id"], vec![]] {
        let mut reader = accessor
            .open(ReadRequest::new("inventory.xml").with_options(options(Some(names.clone()))))
            .await
            .unwrap();
        let mut rows = 0;
        while let Some(batch) = reader.next().await.unwrap() {
            assert_eq!(batch.schema(), reader.metadata().schema);
            assert_eq!(batch.num_columns(), names.len());
            if !names.is_empty() {
                assert_eq!(batch.schema().field(0).name(), "@owner");
                assert_eq!(batch.column(0).as_string::<i32>().value(0), "ExampleCo");
                assert_eq!(batch.column(1).as_primitive::<Int64Type>().value(0), 0);
            }
            rows += batch.num_rows();
        }
        assert_eq!(rows, 2);
    }
    for names in [vec!["missing"], vec!["@id", "@id"]] {
        let error = accessor
            .open(ReadRequest::new("inventory.xml").with_options(options(Some(names))))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
}

#[tokio::test]
async fn catalog_inference_resolves_namespaces_and_optional_fields() {
    let input = open_arrow_access_test_support::fixture!("xml", "catalog.xml").await;
    let accessor =
        XmlAccessor::new(MemoryAccessor::new(&input, 1), CATALOG_PATH).with_namespaces(&[
            NamespaceBinding {
                prefix: "c",
                uri: "urn:open-arrow-access:catalog",
            },
        ]);
    let mut reader = accessor
        .open(
            ReadRequest::new("catalog.xml").with_options(options(Some(vec![
                "c:category",
                "c:name",
                "c:price",
            ]))),
        )
        .await
        .unwrap();
    let mut names = Vec::new();
    let mut categories = Vec::new();
    let mut prices = Vec::new();
    let mut sizes = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.schema(), reader.metadata().schema);
        sizes.push(batch.num_rows());
        prices.extend_from_slice(batch.column(2).as_primitive::<Float64Type>().values());
        names.extend(
            batch
                .column(1)
                .as_string::<i32>()
                .iter()
                .map(|s| s.unwrap().to_owned()),
        );
        categories.extend(
            batch
                .column(0)
                .as_string::<i32>()
                .iter()
                .map(|s| s.map(String::from)),
        );
    }
    assert_eq!(sizes, [2, 2, 1]);
    assert_eq!(prices, [12.50, 39.95, 8.00, 24.50, 45.00]);
    assert_eq!(
        names,
        [
            "Notebook & Pen",
            "Desk Lamp",
            "Café Mug",
            "USB Hub",
            "Travel Bag",
        ]
    );
    assert_eq!(categories[0].as_deref(), Some("stationery"));
    assert!(categories[1..].iter().all(Option::is_none));
}

#[tokio::test]
async fn inference_limits_and_input_failures_preserve_error_categories() {
    let input = open_arrow_access_test_support::fixture!("xml", "inventory.xml").await;
    let bytes = MemoryAccessor::new(&input, 1);
    let observations = bytes.observations.clone();
    for sampling in [
        super::XmlInferenceOptions {
            max_records: 0,
            max_bytes: 4096,
        },
        super::XmlInferenceOptions {
            max_records: 1,
            max_bytes: 0,
        },
    ] {
        let error = XmlAccessor::new(bytes.clone(), "/inventory/warehouse")
            .with_inference_options(sampling)
            .open(ReadRequest::new("inventory.xml"))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
    for path in ["relative/path", "/unbound:warehouse"] {
        let error = XmlAccessor::new(bytes.clone(), path)
            .open(ReadRequest::new("inventory.xml"))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
    assert!(observations.requests.lock().unwrap().is_empty());
    let error = XmlAccessor::new(bytes, "/inventory/warehouse")
        .with_inference_options(super::XmlInferenceOptions {
            max_records: 1,
            max_bytes: 32,
        })
        .open(ReadRequest::new("inventory.xml"))
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::Decode);
    assert!(error.to_string().contains("SampleBytes"));

    let error = XmlAccessor::new(
        MemoryAccessor::new(&input, 1).with_failure(0, ErrorKind::PermissionDenied),
        "/inventory/warehouse",
    )
    .open(ReadRequest::new("inventory.xml"))
    .await
    .err()
    .unwrap();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    let mut reader = XmlAccessor::new(
        MemoryAccessor::new(&input, 1).with_failure(input.len() - 20, ErrorKind::PermissionDenied),
        "/inventory/warehouse",
    )
    .with_inference_options(super::XmlInferenceOptions {
        max_records: 1,
        max_bytes: 4096,
    })
    .open(ReadRequest::new("inventory.xml").with_options(ReadOptions {
        batch_size: NonZeroUsize::MIN,
        projection: None,
    }))
    .await
    .unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
    assert_eq!(
        reader.next().await.unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
    assert!(reader.next().await.unwrap().is_none());

    for name in ["bad_truncated.xml", "bad_entity.xml", "bad_dtd.xml"] {
        let input = open_arrow_access_test_support::fixture!("xml", name).await;
        let error = XmlAccessor::new(MemoryAccessor::new(&input, 1), "/inventory/warehouse")
            .open(ReadRequest::new(name))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::Decode, "{name}");
    }
}
