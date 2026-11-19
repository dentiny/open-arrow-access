use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use std::sync::{Arc, OnceLock};

pub(super) const BODY: &[&str] = &["sentence", "paragraph", "section_header", "bib_ref"];
pub(super) const BIBLIOGRAPHY: &[&str] = &[
    "bib_entry",
    "bib_id",
    "bib_title",
    "bib_venue",
    "bib_author_first_name",
    "bib_author_last_name",
];

fn field(name: &str, ty: DataType) -> Field {
    Field::new(name, ty, true)
}
pub(super) fn attributes() -> Fields {
    vec![
        field("n", DataType::Utf8),
        field("id", DataType::Utf8),
        field("ref_id", DataType::Utf8),
        field("matched_paper_id", DataType::UInt64),
    ]
    .into()
}
pub(super) fn span() -> Fields {
    vec![
        Field::new("start", DataType::UInt64, false),
        Field::new("end", DataType::UInt64, false),
        field("attributes", DataType::Struct(attributes())),
    ]
    .into()
}
fn section(kinds: &[&str], typed: bool) -> DataType {
    let annotations: Fields = kinds
        .iter()
        .map(|name| {
            field(
                name,
                if typed {
                    DataType::List(Arc::new(Field::new(
                        "item",
                        DataType::Struct(span()),
                        false,
                    )))
                } else {
                    DataType::Utf8
                },
            )
        })
        .collect();
    DataType::Struct(
        vec![
            field("text", DataType::LargeUtf8),
            field("annotations", DataType::Struct(annotations)),
        ]
        .into(),
    )
}
fn build(typed: bool) -> SchemaRef {
    let ids: Fields = [
        "Medline",
        "MAG",
        "ACL",
        "DOI",
        "MedRxiv",
        "PubMedCentral",
        "ArXiv",
    ]
    .into_iter()
    .map(|name| field(name, DataType::Utf8))
    .collect();
    let oa = DataType::Struct(
        vec![
            field("disclaimer", DataType::LargeUtf8),
            field("externalids", DataType::Struct(ids)),
            field("license", DataType::Utf8),
            field("url", DataType::Utf8),
            field("status", DataType::Utf8),
        ]
        .into(),
    );
    Arc::new(Schema::new(vec![
        Field::new("corpusid", DataType::UInt64, false),
        field("openaccessinfo", oa),
        field("title", DataType::LargeUtf8),
        field(
            "authors",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
        ),
        field("body", section(BODY, typed)),
        field("bibliography", section(BIBLIOGRAPHY, typed)),
    ]))
}
/// Fixed typed output; JSON-encoded annotation payloads become List<Struct>.
pub fn s2orc_v2_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA.get_or_init(|| build(true)).clone()
}
pub(super) fn input_schema() -> SchemaRef {
    build(false)
}
