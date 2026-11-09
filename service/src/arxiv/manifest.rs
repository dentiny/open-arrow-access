use super::{
    ArxivArchive, ArxivKind,
    error::{decode, xml_error},
    validation::validate_archive,
};
use crate::xml_metadata::XmlDocument;
use arrow_array::{Array, StructArray, cast::AsArray};
use arrow_schema::DataType::Utf8;
use open_arrow_access_core::Result;
use std::{collections::HashSet, str::FromStr};
use xml_arrow::XmlField;

pub(super) async fn parse_manifest(
    bytes: &[u8],
    kind: ArxivKind,
    month: Option<u16>,
) -> Result<Vec<ArxivArchive>> {
    let document = XmlDocument::new(bytes, kind.root())?;
    let fields = ["filename", "yymm", "seq_num", "size", "num_items", "md5sum"]
        .map(|name| XmlField::new(name, document.element(name), Utf8, false));
    let batch = document
        .read(vec![XmlField::list(
            "files",
            ".",
            false,
            XmlField::structure("file", document.element("file"), false, fields.into()),
        )])
        .await?;
    let values = batch.column(0).as_list::<i32>().value(0);
    let files = values.as_struct();
    let mut names = HashSet::new();
    let mut selected = Vec::new();
    for index in 0..files.len() {
        let archive = ArxivArchive {
            kind,
            filename: text(files, "filename", index).into(),
            month: number(files, "yymm", index)?,
            sequence: number(files, "seq_num", index)?,
            size: number(files, "size", index)?,
            num_items: number(files, "num_items", index)?,
            checksum: text(files, "md5sum", index).into(),
        };
        validate_archive(&archive)?;
        if !names.insert(archive.filename.clone()) {
            return Err(decode("duplicate archive in arXiv manifest"));
        }
        if month.is_none_or(|month| archive.month == month) {
            selected.push(archive);
        }
    }
    Ok(selected)
}

fn text<'a>(files: &'a StructArray, field: &str, index: usize) -> &'a str {
    files
        .column_by_name(field)
        .unwrap()
        .as_string::<i32>()
        .value(index)
}

fn number<T: FromStr>(files: &StructArray, field: &str, index: usize) -> Result<T>
where
    T::Err: std::error::Error + Send + Sync + 'static,
{
    text(files, field, index).trim().parse().map_err(xml_error)
}
