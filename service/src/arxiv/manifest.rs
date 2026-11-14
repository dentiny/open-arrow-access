use super::{
    ArxivArchive, ArxivKind,
    error::{decode, xml_error},
    validation::validate_archive,
};
use crate::xml_metadata::XmlDocument;
use arrow_array::{Array, cast::AsArray};
use arrow_schema::DataType::Utf8;
use open_arrow_access_core::{FieldNameIndex, Result};
use open_arrow_access_file_format::xml::XmlField;
use std::{collections::HashSet, str::FromStr};

pub(super) async fn parse_manifest(
    bytes: &[u8],
    kind: ArxivKind,
    month: Option<u16>,
) -> Result<Vec<ArxivArchive>> {
    let root = kind
        .root()
        .ok_or_else(|| decode("manifest kind must be Pdf or Source"))?;
    let document = XmlDocument::new(bytes, root)?;
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
    let field_index = FieldNameIndex::new(files.fields());
    let [filename, month_column, sequence, size, num_items, checksum] =
        ["filename", "yymm", "seq_num", "size", "num_items", "md5sum"].map(|name| {
            files
                .column(field_index.index_of(name).unwrap())
                .as_string::<i32>()
        });
    let mut names = HashSet::new();
    let mut selected = Vec::new();
    for index in 0..files.len() {
        let archive = ArxivArchive {
            kind,
            filename: filename.value(index).into(),
            month: number(month_column.value(index))?,
            sequence: number(sequence.value(index))?,
            size: number(size.value(index))?,
            num_items: number(num_items.value(index))?,
            checksum: checksum.value(index).into(),
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

fn number<T: FromStr>(value: &str) -> Result<T>
where
    T::Err: std::error::Error + Send + Sync + 'static,
{
    value.trim().parse().map_err(xml_error)
}
