use super::{OaiPage, error};
use crate::common::Connection;
use open_arrow_access_core::{Bytes, Result};

#[derive(serde::Deserialize)]
struct Envelope {
    #[serde(rename = "ListRecords")]
    records: Option<Records>,
    error: Option<error::ProtocolError>,
}
#[derive(serde::Deserialize)]
struct Records {
    #[serde(rename = "record", default)]
    records: Vec<serde::de::IgnoredAny>,
    #[serde(rename = "resumptionToken")]
    token: Option<String>,
}
pub(super) fn parse(bytes: Bytes) -> Result<OaiPage> {
    crate::xml_metadata::validate_root(&bytes, "OAI-PMH")?;
    let envelope: Envelope = quick_xml::de::from_reader(bytes.as_ref()).map_err(error::response)?;
    let (record_count, resumption_token) = match (envelope.records, envelope.error) {
        (Some(records), None) => (
            records.records.len(),
            records.token.filter(|s| !s.is_empty()),
        ),
        (None, Some(error)) if error.code == "noRecordsMatch" => (0, None),
        (_, error) => return Err(error::protocol(error)),
    };
    Ok(OaiPage {
        bytes,
        resumption_token,
        record_count,
    })
}

pub(super) fn configuration(connection: &Connection, path: &str, prefix: &str) -> Result<()> {
    connection.url(path, &[])?;
    nonempty(prefix, "metadata prefix")
}
pub(super) fn nonempty(value: &str, field: &str) -> Result<()> {
    if value.is_empty() {
        return Err(error::empty(field));
    }
    Ok(())
}
