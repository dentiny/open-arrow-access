use super::{MecaFile, error, parser::Manifest, validation};
use open_arrow_access_core::Result;
use open_arrow_access_decompression::zip::ZipEntry;

fn resolve_entry_index(entries: &[ZipEntry], name: &[u8]) -> Result<usize> {
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

pub(super) fn resolve_manifest_index(entries: &[ZipEntry]) -> Result<usize> {
    resolve_entry_index(entries, b"manifest.xml")
}

fn resolve_path(href: &str) -> Result<String> {
    validation::validate_href(href)?;
    let path = percent_encoding::percent_decode_str(href)
        .decode_utf8()
        .map_err(error::utf8)?;
    validation::validate_path(&path)?;
    Ok(path.into_owned())
}

pub(super) fn resolve_files(manifest: Manifest, entries: &[ZipEntry]) -> Result<Vec<MecaFile>> {
    let mut files = Vec::new();
    for item in manifest.item {
        validation::validate_item_id(&item.id)?;
        for instance in item.instance {
            let name = resolve_path(&instance.href)?;
            files.push(MecaFile {
                entry_index: resolve_entry_index(entries, name.as_bytes())?,
                item_id: item.id.clone(),
                item_type: item.kind.clone(),
                href: instance.href,
                media_type: instance.media_type,
            });
        }
    }
    Ok(files)
}
