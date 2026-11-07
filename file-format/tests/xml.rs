use arrow_array::{
    Array, RecordBatch,
    cast::AsArray,
    types::{Float64Type, Int64Type},
};
use arrow_schema::DataType::{Float64, Int64, Utf8};
use open_arrow_access_core::{Accessor, ErrorKind, ReadOptions, ReadRequest};
use open_arrow_access_file_format::{
    NamespaceBinding, XmlAccessor, XmlField, XmlInferenceOptions, XmlLimits, XmlSchema,
};
use open_arrow_access_test_support::{
    MemoryAccessor,
    format::{self, collect_batches as batches},
};
use std::{num::NonZeroUsize, sync::atomic::Ordering};

const CATALOG_PATH: &str = "/c:catalog/c:product";
const INVENTORY_PATH: &str = "/inventory/warehouse";
const NAMESPACES: &[NamespaceBinding<'_>] = &[NamespaceBinding {
    prefix: "c",
    uri: "urn:open-arrow-access:catalog",
}];

async fn source(name: &str) -> MemoryAccessor {
    MemoryAccessor::new(
        &open_arrow_access_test_support::fixture!("xml", name).await,
        1,
    )
}

fn catalog_schema() -> XmlSchema {
    XmlSchema::with_namespaces(
        vec![
            XmlField::new("name", "c:name", Utf8, false),
            XmlField::new("price", "c:price", Float64, false),
            XmlField::new("category", "c:category", Utf8, true),
        ],
        NAMESPACES,
    )
    .unwrap()
}

fn warehouse_schema() -> XmlSchema {
    XmlSchema::new(vec![XmlField::new("warehouse", "@id", Int64, false)]).unwrap()
}

fn request(name: &str, rows: usize, projection: Option<&[&str]>) -> ReadRequest {
    ReadRequest::new(name).with_options(ReadOptions {
        batch_size: NonZeroUsize::new(rows).unwrap(),
        projection: projection.map(|names| names.iter().map(|name| (*name).to_owned()).collect()),
    })
}

#[tokio::test]
async fn catalog_reads_namespaces_values_and_order_with_inferred_or_explicit_schema() {
    let accessor =
        XmlAccessor::new(source("catalog.xml").await, CATALOG_PATH).with_namespaces(NAMESPACES);
    for (accessor, columns) in [
        (accessor.clone(), ["c:category", "c:name", "c:price"]),
        (
            accessor.with_schema(catalog_schema()),
            ["category", "name", "price"],
        ),
    ] {
        let batches = batches(&accessor, request("catalog.xml", 2, Some(&columns))).await;
        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            [2, 2, 1]
        );
        let mut values = Vec::new();
        for batch in &batches {
            assert_eq!(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>(),
                columns
            );
            let categories = batch.column(0).as_string::<i32>();
            let names = batch.column(1).as_string::<i32>();
            let prices = batch.column(2).as_primitive::<Float64Type>();
            values.extend((0..batch.num_rows()).map(|i| {
                (
                    names.value(i).to_owned(),
                    prices.value(i),
                    (!categories.is_null(i)).then(|| categories.value(i).to_owned()),
                )
            }));
        }
        assert_eq!(
            values,
            [
                ("Notebook & Pen".into(), 12.50, Some("stationery".into())),
                ("Desk Lamp".into(), 39.95, None),
                ("Café Mug".into(), 8.00, None),
                ("USB Hub".into(), 24.50, None),
                ("Travel Bag".into(), 45.00, None),
            ]
        );
    }
}

#[tokio::test]
async fn path_only_infers_nested_lists_and_replays_bounded_sample() {
    let input = open_arrow_access_test_support::fixture!("xml", "inventory.xml").await;
    let bytes = MemoryAccessor::new(&input, 1);
    let observations = bytes.observations.clone();
    let accessor =
        XmlAccessor::new(bytes, INVENTORY_PATH).with_inference_options(XmlInferenceOptions {
            max_records: 1,
            max_bytes: 4096,
        });
    let mut reader = accessor
        .open(request("inventory.xml", 1, None))
        .await
        .unwrap();
    assert!(observations.reads.load(Ordering::Relaxed) < input.len());
    let schema = reader.metadata().schema.clone();
    for (id, location) in ["north", "south"].into_iter().enumerate() {
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.schema(), schema);
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column_by_name("@id")
                .unwrap()
                .as_primitive::<Int64Type>()
                .value(0),
            id as i64
        );
        let racks = batch.column_by_name("rack").unwrap().as_list::<i32>();
        assert_eq!(racks.value_offsets(), &[0, 2]);
        let racks = racks.values().as_struct();
        assert_eq!(
            racks
                .column_by_name("@capacity")
                .unwrap()
                .as_primitive::<Int64Type>()
                .values()
                .as_ref(),
            &[16, 16]
        );
        let devices = racks.column_by_name("device").unwrap().as_list::<i32>();
        assert_eq!(devices.value_offsets(), &[0, 2, 4]);
        let devices = devices.values().as_struct();
        let ids = devices.column_by_name("@id").unwrap().as_string::<i32>();
        let expected =
            ["gpu-1", "nic-1", "gpu-2", "nic-2"].map(|name| format!("{location}-{name}"));
        assert_eq!(
            ids.iter().collect::<Vec<_>>(),
            expected
                .iter()
                .map(|s| Some(s.as_str()))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            devices
                .column_by_name("@kind")
                .unwrap()
                .as_string::<i32>()
                .iter()
                .collect::<Vec<_>>(),
            [Some("gpu"), Some("network"), Some("gpu"), Some("network")]
        );
    }
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn projection_skips_explicit_conversion_and_preserves_zero_column_rows() {
    let bytes = source("catalog.xml").await;
    let mut fields = catalog_schema().fields().to_vec();
    fields.push(XmlField::new("unused_number", "c:price", Int64, false));
    let schema = XmlSchema::with_namespaces(fields, NAMESPACES).unwrap();
    let explicit = XmlAccessor::new(bytes.clone(), CATALOG_PATH).with_schema(schema.clone());
    // Decimal prices cannot convert to Int64, but omitted fields are not converted.
    let selected = format::assert_projection(
        &explicit,
        request("catalog.xml", 2, Some(&["name"])),
        &schema.arrow_schema(),
        5,
    )
    .await;
    assert_eq!(
        selected[0].column(0).as_string::<i32>().value(0),
        "Notebook & Pen"
    );
    for accessor in [
        explicit,
        XmlAccessor::new(bytes, CATALOG_PATH).with_namespaces(NAMESPACES),
    ] {
        let batches = batches(&accessor, request("catalog.xml", 2, Some(&[]))).await;
        assert!(batches.iter().all(|batch| batch.num_columns() == 0));
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 5);
    }
}

#[tokio::test]
async fn invalid_configuration_fails_before_opening_input() {
    let bytes = source("inventory.xml").await;
    let observations = bytes.observations.clone();
    let accessor = XmlAccessor::new(bytes.clone(), INVENTORY_PATH);
    for invalid in [
        XmlAccessor::new(bytes.clone(), "relative/path"),
        XmlAccessor::new(bytes.clone(), "/unbound:warehouse"),
        accessor.clone().with_limits(XmlLimits {
            max_record_bytes: 50,
            max_batch_bytes: 49,
            max_depth: 128,
        }),
        accessor
            .clone()
            .with_inference_options(XmlInferenceOptions {
                max_records: 0,
                max_bytes: 4096,
            }),
        accessor
            .clone()
            .with_inference_options(XmlInferenceOptions {
                max_records: 1,
                max_bytes: 0,
            }),
    ] {
        let error = invalid
            .open(request("inventory.xml", 2, None))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
    let explicit = accessor.with_schema(warehouse_schema());
    format::assert_invalid_projection_before_open(
        &explicit,
        request("inventory.xml", 2, None),
        &warehouse_schema().arrow_schema(),
        &observations,
    )
    .await;
    assert!(observations.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn inferred_projection_rejects_unknown_or_duplicate_columns() {
    let accessor = XmlAccessor::new(source("inventory.xml").await, INVENTORY_PATH);
    for names in [&["missing"][..], &["@id", "@id"]] {
        let error = accessor
            .open(request("inventory.xml", 2, Some(names)))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
}

#[tokio::test]
async fn malformed_inputs_fail_during_sampling_or_terminate_explicit_readers() {
    for name in [
        "bad_integer.xml",
        "bad_truncated.xml",
        "bad_entity.xml",
        "bad_dtd.xml",
    ] {
        let accessor = XmlAccessor::new(source(name).await, INVENTORY_PATH);
        if name != "bad_integer.xml" {
            let error = accessor.open(request(name, 2, None)).await.err().unwrap();
            assert_eq!(error.kind(), ErrorKind::Decode, "{name}");
        }
        // Read beyond both fixture records to observe errors in the document tail.
        let mut reader = accessor
            .with_schema(warehouse_schema())
            .open(request(name, 3, None))
            .await
            .unwrap();
        let error = reader.next().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode, "{name}");
        assert!(std::error::Error::source(&error).is_some());
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn parsing_limits_report_decode_errors() {
    let accessor = XmlAccessor::new(source("inventory.xml").await, INVENTORY_PATH);
    let error = accessor
        .clone()
        .with_inference_options(XmlInferenceOptions {
            max_records: 1,
            max_bytes: 32,
        })
        .open(request("inventory.xml", 2, None))
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::Decode);
    assert!(error.to_string().contains("SampleBytes"));
    let explicit = accessor.with_schema(warehouse_schema());
    for (limits, expected) in [
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
        let mut reader = explicit
            .clone()
            .with_limits(limits)
            .open(request("inventory.xml", 2, None))
            .await
            .unwrap();
        let error = reader.next().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode);
        assert!(error.to_string().contains(expected));
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn batch_byte_budget_preserves_lookahead_order() {
    let limited = XmlAccessor::new(source("inventory.xml").await, INVENTORY_PATH)
        .with_schema(warehouse_schema())
        .with_limits(XmlLimits {
            max_record_bytes: 512,
            max_batch_bytes: 512,
            max_depth: 128,
        });
    let batches = batches(&limited, request("inventory.xml", 2, None)).await;
    assert_eq!(batches.len(), 2);
    for (id, batch) in batches.iter().enumerate() {
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch.column(0).as_primitive::<Int64Type>().value(0),
            id as i64
        );
    }
}

#[tokio::test]
async fn input_failures_keep_their_category_during_sampling_and_later_reads() {
    let input = open_arrow_access_test_support::fixture!("xml", "inventory.xml").await;
    format::assert_error_contract(
        |bytes| XmlAccessor::new(bytes, INVENTORY_PATH),
        &input,
        request("inventory.xml", 1, None),
        &[0, 1],
    )
    .await;
    format::assert_error_contract(
        |bytes| XmlAccessor::new(bytes, INVENTORY_PATH).with_schema(warehouse_schema()),
        &input,
        request("inventory.xml", 1, None),
        &[0, 1],
    )
    .await;
    let accessor = XmlAccessor::new(
        MemoryAccessor::new(&input, 1).with_failure(input.len() - 20, ErrorKind::PermissionDenied),
        INVENTORY_PATH,
    )
    .with_inference_options(XmlInferenceOptions {
        max_records: 1,
        max_bytes: 4096,
    });
    let mut reader = accessor
        .open(request("inventory.xml", 1, None))
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
    assert_eq!(
        reader.next().await.unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
    assert!(reader.next().await.unwrap().is_none());
}
