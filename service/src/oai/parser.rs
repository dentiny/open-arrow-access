use super::{OaiPage, error, validation};
use open_arrow_access_core::{Bytes, Result};

#[derive(serde::Deserialize)]
pub(super) struct Envelope {
    #[serde(rename = "ListRecords")]
    pub(super) records: Option<Records>,
    pub(super) error: Option<error::ProtocolError>,
}

#[derive(serde::Deserialize)]
pub(super) struct Records {
    #[serde(rename = "record", default)]
    records: Vec<serde::de::IgnoredAny>,
    #[serde(rename = "resumptionToken")]
    token: Option<String>,
}

pub(super) fn parse_page(bytes: Bytes) -> Result<OaiPage> {
    validation::validate_root(&bytes)?;
    let envelope: Envelope = quick_xml::de::from_reader(bytes.as_ref()).map_err(error::response)?;
    validation::validate_response(&envelope)?;
    let records = envelope.records;
    Ok(OaiPage {
        bytes,
        record_count: records.as_ref().map_or(0, |records| records.records.len()),
        resumption_token: records
            .and_then(|records| records.token)
            .filter(|token| !token.is_empty()),
    })
}
