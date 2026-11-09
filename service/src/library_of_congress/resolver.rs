use super::{error::decode, validation};
use crate::common::{
    Connection, LinkField, MetadataLink, encode_path, resolve_download_url, resolve_optional_links,
};
use open_arrow_access_core::Result;
use serde_json::Value;
use url::Url;

fn resolve_json_url(connection: &Connection, mut url: Url) -> Result<Url> {
    if url.scheme() == "http"
        && connection.origin().scheme() == "https"
        && url.host_str() == connection.origin().host_str()
        && url.port().is_none()
        && connection.origin().port().is_none()
    {
        url.set_scheme("https").map_err(super::error::scheme)?;
    }
    let query = url
        .query_pairs()
        .into_owned()
        .filter(|(name, _)| name != "fo" && name != "at")
        .collect::<Vec<_>>();
    url.set_query(None);
    url.query_pairs_mut()
        .extend_pairs(query)
        .append_pair("fo", "json");
    Ok(url)
}

pub(super) fn resolve_continuation_url(connection: &Connection, value: &str) -> Result<Url> {
    resolve_json_url(connection, resolve_download_url(value)?)
}

pub(super) fn resolve_metadata_url(
    connection: &Connection,
    kind: &str,
    value: &str,
) -> Result<Url> {
    let prefix = format!("/{kind}/");
    let url = if value.contains("://") {
        resolve_download_url(value)?
    } else if value.starts_with(&prefix) {
        connection.origin().join(value).map_err(super::error::url)?
    } else {
        let id = encode_path(value.trim_end_matches('/'))?;
        connection.url(&format!("{kind}/{id}/"), &[])?
    };
    validation::validate_metadata_endpoint(&url, &prefix)?;
    resolve_json_url(connection, url)
}

fn resolve_links(value: &Value, fields: &[&str], prefix: &str) -> Result<Vec<MetadataLink>> {
    let mut selected = serde_json::Map::new();
    for field in fields {
        if let Some(value) = value.get(*field) {
            let value = match value {
                Value::String(url) if url.starts_with("//") => {
                    Value::String(format!("https:{url}"))
                }
                _ => value.clone(),
            };
            selected.insert((*field).into(), value);
        }
    }
    let selectors = fields
        .iter()
        .map(|field| LinkField {
            json_field: field,
            file_name: field,
        })
        .collect::<Vec<_>>();
    Ok(
        resolve_optional_links(&Value::Object(selected), &selectors)?
            .into_iter()
            .map(|link| MetadataLink {
                file_name: format!("{prefix}/{}", link.file_name),
                ..link
            })
            .collect(),
    )
}

pub(super) fn resolve_resource_files(value: &Value) -> Result<Vec<MetadataLink>> {
    let resource = value
        .get("resource")
        .filter(|r| r.is_object())
        .ok_or_else(|| decode("missing LoC resource metadata"))?;
    let root_fields = [
        "pdf",
        "fulltext_file",
        "fulltext_derivative",
        "djvu_text_file",
        "searchable_text_file",
        "word_coordinates",
    ];
    let mut files = resolve_links(resource, &root_fields, "resource")?;
    if let Some(groups) = resource.get("files") {
        if groups.as_u64() == Some(0) {
            return Ok(files);
        }
        for (group_index, group) in groups
            .as_array()
            .ok_or_else(|| decode("LoC resource files are not expanded"))?
            .iter()
            .enumerate()
        {
            let representations = if let Some(group) = group.as_array() {
                group.as_slice()
            } else {
                std::slice::from_ref(group)
            };
            for (index, representation) in representations.iter().enumerate() {
                validation::validate_representation(representation)?;
                let fields = [
                    "url",
                    "filename",
                    "pdf",
                    "fulltext_file",
                    "fulltext_derivative",
                    "djvu_text_file",
                    "word_coordinates",
                    "download",
                    "video_stream",
                    "image",
                ];
                files.extend(resolve_links(
                    representation,
                    &fields,
                    &format!("file-{group_index:05}/representation-{index:03}"),
                )?);
            }
        }
    }
    Ok(files)
}
