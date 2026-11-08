use std::collections::HashSet;

use open_arrow_access_core::Result;
use quick_xml::{Reader, events::Event};
use serde::Deserialize;

use super::{
    ArxivArchive, ArxivKind,
    error::{decode, xml_error},
    validation::{validate_archive, validate_root},
};

pub(super) fn parse_manifest(
    bytes: &[u8],
    kind: ArxivKind,
    month: Option<u16>,
) -> Result<Vec<ArxivArchive>> {
    let mut reader = Reader::from_reader(bytes);
    let mut depth = 0_usize;
    let mut root_seen = false;
    let mut declaration_seen = false;
    loop {
        match reader.read_event().map_err(xml_error)? {
            Event::Start(start) => {
                if depth == 0 {
                    validate_root(start.name().as_ref(), kind, root_seen)?;
                    root_seen = true;
                }
                depth += 1;
            }
            Event::Empty(start) if depth == 0 => {
                validate_root(start.name().as_ref(), kind, root_seen)?;
                root_seen = true;
            }
            Event::End(_) => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| decode("unexpected manifest closing tag"))?;
            }
            Event::Text(text) if depth == 0 && !text.iter().all(u8::is_ascii_whitespace) => {
                return Err(decode("unexpected text outside arXiv manifest root"));
            }
            Event::CData(_) | Event::GeneralRef(_) if depth == 0 => {
                return Err(decode("unexpected content outside manifest root"));
            }
            Event::DocType(_) => return Err(decode("manifest document types are unsupported")),
            Event::Decl(_) => {
                if root_seen || declaration_seen {
                    return Err(decode("unexpected manifest XML declaration"));
                }
                declaration_seen = true;
            }
            Event::Eof => {
                if !root_seen || depth != 0 {
                    return Err(decode("incomplete arXiv manifest root"));
                }
                break;
            }
            _ => (),
        }
    }
    let manifest: Manifest = quick_xml::de::from_reader(bytes).map_err(xml_error)?;
    let mut names = HashSet::new();
    let mut selected = Vec::new();
    for file in manifest.files {
        let archive = ArxivArchive {
            kind,
            filename: file.filename,
            month: file.yymm,
            sequence: file.seq_num,
            size: file.size,
            num_items: file.num_items,
            first_item: file.first_item,
            last_item: file.last_item,
            checksum: file.md5sum,
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

#[derive(Deserialize)]
struct Manifest {
    #[serde(rename = "file", default)]
    files: Vec<ManifestFile>,
}

#[derive(Deserialize)]
struct ManifestFile {
    filename: String,
    yymm: u16,
    seq_num: u32,
    size: u64,
    num_items: u64,
    first_item: String,
    last_item: String,
    md5sum: String,
}
