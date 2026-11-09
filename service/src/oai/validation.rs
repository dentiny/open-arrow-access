use super::{error, parser::Envelope};
use open_arrow_access_core::Result;

pub(super) fn validate_response(envelope: &Envelope) -> Result<()> {
    match (envelope.records.as_ref(), envelope.error.as_ref()) {
        (Some(_), None) => Ok(()),
        (None, Some(error)) if error.code == "noRecordsMatch" => Ok(()),
        (_, error) => Err(error::protocol(error.cloned())),
    }
}

pub(super) fn validate_nonempty_text(value: &str, field: &str) -> Result<()> {
    if value.is_empty() {
        return Err(error::empty(field));
    }
    Ok(())
}
