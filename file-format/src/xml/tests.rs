use super::{NamespaceBinding, XmlAccessor, XmlField, XmlLimits, XmlSchema};
use arrow_array::{Array, cast::AsArray, types::Int64Type};
use arrow_schema::DataType::{Int64, Utf8};
use open_arrow_access_core::{Accessor, ErrorKind, ReadOptions, ReadRequest};
use open_arrow_access_test_support::MemoryAccessor;
use std::num::NonZeroUsize;

const POM_PATH: &str = "/m:project/m:dependencyManagement/m:dependencies/m:dependency";

fn pom_schema() -> XmlSchema {
    XmlSchema::with_namespaces(
        vec![
            XmlField::new("artifact", "m:artifactId", Utf8, false),
            XmlField::new("version", "m:version", Utf8, false),
            XmlField::new("scope", "m:scope", Utf8, true),
        ],
        &[NamespaceBinding {
            prefix: "m",
            uri: "http://maven.apache.org/POM/4.0.0",
        }],
    )
    .unwrap()
}

fn cpu_schema() -> XmlSchema {
    XmlSchema::new(vec![XmlField::new("numa", "@numaid", Int64, false)]).unwrap()
}

fn options(projection: Option<Vec<&str>>) -> ReadOptions {
    ReadOptions {
        batch_size: NonZeroUsize::new(2).unwrap(),
        projection: projection.map(|names| names.into_iter().map(String::from).collect()),
    }
}

#[tokio::test]
async fn fragmented_real_pom_preserves_namespaces_values_schema_and_order() {
    let input = open_arrow_access_test_support::fixture!("xml", "lancedb-pom.xml").await;
    let bytes = MemoryAccessor::new(&input, 1);
    let accessor = XmlAccessor::new(bytes, pom_schema(), POM_PATH);
    let mut reader = accessor
        .open(ReadRequest::new("lancedb-pom.xml").with_options(options(None)))
        .await
        .unwrap();
    assert_eq!(reader.metadata().schema, pom_schema().arrow_schema());
    let mut artifacts = Vec::new();
    let mut sizes = Vec::new();
    let mut scopes = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.schema(), reader.metadata().schema);
        sizes.push(batch.num_rows());
        artifacts.extend(
            batch
                .column(0)
                .as_string::<i32>()
                .iter()
                .map(|s| s.unwrap().to_owned()),
        );
        scopes.extend(
            batch
                .column(2)
                .as_string::<i32>()
                .iter()
                .map(|s| s.map(String::from)),
        );
        if sizes.len() == 1 {
            assert_eq!(
                batch.column(1).as_string::<i32>().value(0),
                "${jackson.version}"
            );
        }
    }
    assert_eq!(sizes, [2, 2, 2, 1]);
    assert_eq!(
        artifacts,
        [
            "jackson-bom",
            "lance-core",
            "arrow-vector",
            "arrow-memory-netty",
            "arrow-c-data",
            "arrow-dataset",
            "junit-jupiter"
        ]
    );
    assert_eq!(scopes[0].as_deref(), Some("import"));
    assert!(scopes[1..].iter().all(Option::is_none));
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn real_topology_preserves_nested_device_lists() {
    let input = open_arrow_access_test_support::fixture!("xml", "azure-ndv5-topology.xml").await;
    let schema = XmlSchema::new(vec![
        XmlField::new("numa", "@numaid", Int64, false),
        XmlField::list(
            "bridges",
            ".",
            false,
            XmlField::structure(
                "item",
                "pci",
                false,
                vec![
                    XmlField::new("width", "@link_width", Int64, false),
                    XmlField::list(
                        "devices",
                        ".",
                        false,
                        XmlField::structure(
                            "item",
                            "pci",
                            false,
                            vec![
                                XmlField::new("bus", "@busid", Utf8, false),
                                XmlField::new("class", "@class", Utf8, false),
                            ],
                        ),
                    ),
                ],
            ),
        ),
    ])
    .unwrap();
    let accessor = XmlAccessor::new(MemoryAccessor::new(&input, 1), schema, "/system/cpu");
    let mut reader = accessor
        .open(ReadRequest::new("topology.xml").with_options(ReadOptions {
            batch_size: NonZeroUsize::MIN,
            projection: None,
        }))
        .await
        .unwrap();
    for numa in 0..2 {
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), numa);
        let bridges = batch.column(1).as_list::<i32>();
        assert_eq!(bridges.value_offsets(), &[0, 4]);
        let bridges = bridges.values().as_struct();
        assert_eq!(
            bridges
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .as_ref(),
            &[16; 4]
        );
        let devices = bridges.column(1).as_list::<i32>();
        assert_eq!(devices.value_offsets(), &[0, 2, 4, 6, 8]);
        let devices = devices.values().as_struct();
        assert_eq!(devices.len(), 8);
        assert_eq!(devices.column(1).as_string::<i32>().value(0), "0x030200");
        assert_eq!(devices.column(1).as_string::<i32>().value(1), "0x020700");
        let bus = devices.column(0).as_string::<i32>();
        assert_eq!(
            bus.value(0),
            if numa == 0 {
                "0001:00:00.0"
            } else {
                "0009:00:00.0"
            }
        );
        assert_eq!(
            bus.value(7),
            if numa == 0 {
                "0104:00:00.0"
            } else {
                "0108:00:00.0"
            }
        );
    }
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn real_pom_projection_skips_conversion_and_counts_zero_columns() {
    let input = open_arrow_access_test_support::fixture!("xml", "lancedb-pom.xml").await;
    let mut fields = pom_schema().fields().to_vec();
    fields.push(XmlField::new("unused_number", "m:version", Int64, false));
    let schema = XmlSchema::with_namespaces(
        fields,
        &[NamespaceBinding {
            prefix: "m",
            uri: "http://maven.apache.org/POM/4.0.0",
        }],
    )
    .unwrap();
    let accessor = XmlAccessor::new(MemoryAccessor::new(&input, 3), schema, POM_PATH);
    let mut reader = accessor
        .open(ReadRequest::new("pom.xml").with_options(options(Some(vec!["scope", "artifact"]))))
        .await
        .unwrap();
    assert_eq!(reader.metadata().schema.field(0).name(), "scope");
    let first = reader.next().await.unwrap().unwrap();
    assert_eq!(first.column(0).as_string::<i32>().value(0), "import");
    assert_eq!(first.column(1).as_string::<i32>().value(1), "lance-core");
    let mut reader = accessor
        .open(ReadRequest::new("pom.xml").with_options(options(Some(vec![]))))
        .await
        .unwrap();
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.num_columns(), 0);
        rows += batch.num_rows();
    }
    assert_eq!(rows, 7);
}

#[tokio::test]
async fn invalid_configuration_fails_before_opening_input() {
    let input = open_arrow_access_test_support::fixture!("xml", "lancedb-pom.xml").await;
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
        let accessor = XmlAccessor::new(bytes.clone(), pom_schema(), path).with_limits(limits);
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
    let accessor = XmlAccessor::new(bytes, pom_schema(), POM_PATH);
    for names in [vec!["missing"], vec!["artifact", "artifact"]] {
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
        let accessor =
            XmlAccessor::new(MemoryAccessor::new(&input, 1), cpu_schema(), "/system/cpu");
        let mut reader = accessor.open(ReadRequest::new(name)).await.unwrap();
        let error = reader.next().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode, "{name}");
        assert!(std::error::Error::source(&error).is_some());
        assert!(reader.next().await.unwrap().is_none());
    }
    let input = open_arrow_access_test_support::fixture!("xml", "azure-ndv5-topology.xml").await;
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
        let accessor =
            XmlAccessor::new(MemoryAccessor::new(&input, 1), cpu_schema(), "/system/cpu")
                .with_limits(limits);
        let mut reader = accessor
            .open(ReadRequest::new("topology.xml"))
            .await
            .unwrap();
        let error = reader.next().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode);
        assert!(error.to_string().contains(kind));
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn real_topology_batch_byte_limits_preserve_lookahead_order() {
    let input = open_arrow_access_test_support::fixture!("xml", "azure-ndv5-topology.xml").await;
    let accessor = XmlAccessor::new(MemoryAccessor::new(&input, 1), cpu_schema(), "/system/cpu")
        .with_limits(XmlLimits {
            max_record_bytes: 2048,
            max_batch_bytes: 2048,
            max_depth: 128,
        });
    let mut reader = accessor
        .open(ReadRequest::new("topology.xml"))
        .await
        .unwrap();
    for numa in 0..2 {
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), numa);
    }
    assert!(reader.next().await.unwrap().is_none());
}
