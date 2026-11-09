use super::{MecaFile, error};
use open_arrow_access_core::Result;
use open_arrow_access_decompression::zip::ZipEntry;
use serde::Deserialize;
#[derive(Deserialize)]
struct Manifest {
    #[serde(default)]
    item: Vec<Item>,
}
#[derive(Deserialize)]
struct Item {
    #[serde(rename = "@id")]
    id: String,
    #[serde(rename = "@item-type")]
    kind: Option<String>,
    #[serde(default)]
    instance: Vec<Instance>,
}
#[derive(Deserialize)]
struct Instance {
    #[serde(rename = "@href")]
    href: String,
    #[serde(rename = "@media-type")]
    media_type: Option<String>,
}

fn unique(entries: &[ZipEntry], name: &[u8]) -> Result<usize> {
    let mut matches = entries.iter().filter(|entry| entry.name == name);
    let index = matches
        .next()
        .ok_or_else(|| error::decode("manifest member is missing"))?
        .index;
    if matches.next().is_some() {
        return Err(error::decode("manifest member is ambiguous"));
    }
    Ok(index)
}

pub(super) fn manifest_index(entries: &[ZipEntry]) -> Result<usize> {
    unique(entries, b"manifest.xml")
}

fn path(href: &str) -> Result<String> {
    if url::Url::parse(href).is_ok() || href.contains(['?', '#']) {
        return Err(error::decode("MECA href must be a relative member URI"));
    }
    let path = percent_encoding::percent_decode_str(href)
        .decode_utf8()
        .map_err(error::utf8)?;
    if !crate::validation::valid_relative_file_path(&path) {
        return Err(error::decode("unsafe MECA member path"));
    }
    Ok(path.into_owned())
}

pub(super) fn files(bytes: &[u8], entries: &[ZipEntry]) -> Result<Vec<MecaFile>> {
    crate::xml_metadata::validate_root(bytes, "manifest")?;
    let manifest: Manifest = quick_xml::de::from_reader(bytes).map_err(error::xml)?;
    let mut files = Vec::new();
    for item in manifest.item {
        if item.id.is_empty() {
            return Err(error::decode("MECA item id is empty"));
        }
        for instance in item.instance {
            let name = path(&instance.href)?;
            files.push(MecaFile {
                entry_index: unique(entries, name.as_bytes())?,
                item_id: item.id.clone(),
                item_type: item.kind.clone(),
                href: instance.href,
                media_type: instance.media_type,
            });
        }
    }
    Ok(files)
}
