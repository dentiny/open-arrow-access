use super::{error, parser::Envelope};
use crate::common::Connection;
use open_arrow_access_core::Result;

pub(super) fn validate_root(bytes: &[u8]) -> Result<()> {
    crate::xml_metadata::validate_root(bytes, "OAI-PMH")
}

pub(super) fn validate_response(envelope: &Envelope) -> Result<()> {
    match (envelope.records.as_ref(), envelope.error.as_ref()) {
        (Some(_), None) => Ok(()),
        (None, Some(error)) if error.code == "noRecordsMatch" => Ok(()),
        (_, error) => Err(error::protocol(error.cloned())),
    }
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
