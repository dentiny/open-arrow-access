use crate::xml_metadata::XmlDocument;
use arrow_array::{Array, StructArray, cast::AsArray};
use arrow_schema::DataType::Utf8;
use open_arrow_access_core::Result;
use open_arrow_access_file_format::xml::XmlField;

pub(super) struct Manifest {
    pub(super) item: Vec<Item>,
}

pub(super) struct Item {
    pub(super) id: String,
    pub(super) kind: Option<String>,
    pub(super) instance: Vec<Instance>,
}

pub(super) struct Instance {
    pub(super) href: String,
    pub(super) media_type: Option<String>,
}

pub(super) async fn parse_manifest(bytes: &[u8]) -> Result<Manifest> {
    let document = XmlDocument::new(bytes, "manifest")?;
    let batch = document
        .read(vec![XmlField::list(
            "items",
            ".",
            false,
            XmlField::structure(
                "item",
                document.element("item"),
                false,
                vec![
                    XmlField::new("id", "@id", Utf8, false),
                    XmlField::new("kind", "@item-type", Utf8, true),
                    XmlField::list(
                        "instances",
                        ".",
                        false,
                        XmlField::structure(
                            "instance",
                            document.element("instance"),
                            false,
                            vec![
                                XmlField::new("href", "@href", Utf8, false),
                                XmlField::new("media_type", "@media-type", Utf8, true),
                            ],
                        ),
                    ),
                ],
            ),
        )])
        .await?;
    let values = batch.column(0).as_list::<i32>().value(0);
    let items = values.as_struct();
    let item = (0..items.len())
        .map(|index| {
            let values = items.column(2).as_list::<i32>().value(index);
            let instances = values.as_struct();
            Item {
                id: items.column(0).as_string::<i32>().value(index).into(),
                kind: optional_text(items, 1, index),
                instance: (0..instances.len())
                    .map(|index| Instance {
                        href: instances.column(0).as_string::<i32>().value(index).into(),
                        media_type: optional_text(instances, 1, index),
                    })
                    .collect(),
            }
        })
        .collect();
    Ok(Manifest { item })
}

fn optional_text(values: &StructArray, column: usize, index: usize) -> Option<String> {
    let values = values.column(column).as_string::<i32>();
    (!values.is_null(index)).then(|| values.value(index).to_owned())
}
