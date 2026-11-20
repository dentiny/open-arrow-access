use super::*;
use arrow_array::{Array, LargeStringArray, ListArray};
use flate2::{Compression, write::GzEncoder};
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::temp_fs;
use std::{collections::BTreeMap, io::Write};

async fn fixture() -> (tempfile::TempDir, std::path::PathBuf, BhlMetadataInfo) {
    let rows: BTreeMap<&str, Vec<Vec<(&str, &str)>>> = BTreeMap::from([
        (
            "title",
            vec![vec![
                ("TitleID", "001"),
                ("FullTitle", "Original \"title\" 遗传"),
            ]],
        ),
        (
            "item",
            vec![vec![
                ("ItemID", "2"),
                ("TitleID", "1"),
                ("RightsStatement", "Source rights"),
            ]],
        ),
        (
            "page",
            vec![vec![
                ("PageID", "3"),
                ("ItemID", "2"),
                ("SequenceOrder", "5"),
            ]],
        ),
        (
            "part",
            vec![vec![
                ("PartID", "8"),
                ("ItemID", "2"),
                ("SequenceOrder", "900"),
                ("RightsStatement", "Part rights"),
            ]],
        ),
        (
            "partpage",
            vec![vec![
                ("PartID", "8"),
                ("PageID", "3"),
                ("ItemID", "2"),
                ("SequenceOrder", "1"),
            ]],
        ),
        ("creator", vec![vec![("TitleID", "1"), ("CreatorID", "9")]]),
        (
            "partcreator",
            vec![vec![("PartID", "8"), ("CreatorID", "9")]],
        ),
        (
            "creatoridentifier",
            vec![
                vec![("CreatorID", "9"), ("IdentifierValue", "same")],
                vec![("CreatorID", "9"), ("IdentifierValue", "same")],
            ],
        ),
        (
            "doi",
            vec![
                vec![
                    ("EntityType", "Title"),
                    ("EntityID", "1"),
                    ("DOI", "title DOI"),
                ],
                vec![
                    ("EntityType", "Part"),
                    ("EntityID", "1"),
                    ("DOI", "wrong entity"),
                ],
            ],
        ),
    ]);
    let files = tables()
        .into_iter()
        .map(|table| {
            let mut content = table
                .fields
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>()
                .join("\t")
                + "\n";
            for row in rows.get(table.name.as_str()).into_iter().flatten() {
                let values = row.iter().copied().collect::<BTreeMap<_, _>>();
                content += &(table
                    .fields
                    .iter()
                    .map(|f| values.get(f.name().as_str()).copied().unwrap_or(""))
                    .collect::<Vec<_>>()
                    .join("\t")
                    + "\n");
            }
            let mut gzip = GzEncoder::new(Vec::new(), Compression::fast());
            gzip.write_all(content.as_bytes()).unwrap();
            (
                format!("data/{}.txt.gz", table.name),
                gzip.finish().unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let borrowed = files
        .iter()
        .map(|(p, b)| (p.as_str(), b.as_slice()))
        .collect::<Vec<_>>();
    let (_source, operator) = temp_fs(&borrowed);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("metadata.sqlite");
    let info = prepare(OpenDalAccessor::new(operator), "s3://bhl-open-data/", &path).await;
    (root, path, info.unwrap())
}
fn cell<'a>(metadata: &'a StructArray, table: &str, field: &str) -> &'a str {
    metadata
        .column_by_name(table)
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .column_by_name(field)
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap()
        .value(0)
}
#[tokio::test]
async fn joins_indexed_metadata_preserves_rights_and_reports_gaps() {
    let (_root, path, info) = fixture().await;
    assert_eq!(inspect(&path).unwrap().fingerprint, info.fingerprint);
    assert_eq!(info.sources.len(), 13);
    let keys = [
        "ocr/item-2/item-2-3-5.txt",
        "ocr/part-8/part-8-3-1.txt",
        "ocr/item-20/item-20-30-1.txt",
    ]
    .map(ObjectRef::new);
    let selected = BhlPageMetadata::load(&path, &info.fingerprint, &keys).unwrap();
    assert!(selected.report.conflicts.is_empty());
    assert_eq!(selected.report.missing.len(), 1);
    for key in &keys[..2] {
        let metadata = selected
            .get(&BhlPageIdentity::parse(key.locator()).unwrap())
            .unwrap();
        assert!(!metadata.is_null(0));
        assert_eq!(cell(&metadata, "title", "TitleID"), "001");
        assert_eq!(
            cell(&metadata, "title", "FullTitle"),
            "Original \"title\" 遗传"
        );
        assert_eq!(cell(&metadata, "item", "RightsStatement"), "Source rights");
        let identifiers = metadata
            .column_by_name("creatoridentifier")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(identifiers.value_length(0), 2);
        let doi = metadata
            .column_by_name("doi")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(doi.value_length(0), 1);
    }
    let part = selected
        .get(&BhlPageIdentity::parse(keys[1].locator()).unwrap())
        .unwrap();
    assert_eq!(cell(&part, "part", "RightsStatement"), "Part rights");
    let missing = selected
        .get(&BhlPageIdentity::parse(keys[2].locator()).unwrap())
        .unwrap();
    assert!(!missing.is_null(0));
    assert!(missing.column_by_name("item").unwrap().is_null(0));
    assert!(BhlPageMetadata::load(&path, "wrong", &keys).is_err());
    let conflict = [ObjectRef::new("ocr/item-2/item-2-3-99.txt")];
    let selected = BhlPageMetadata::load(&path, &info.fingerprint, &conflict).unwrap();
    assert_eq!(selected.report.conflicts.len(), 1);
    assert!(
        BhlPageMetadata::load(
            &path.with_file_name("missing.sqlite"),
            &info.fingerprint,
            &keys
        )
        .is_err()
    );
}
