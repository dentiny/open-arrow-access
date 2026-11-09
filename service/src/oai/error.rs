use open_arrow_access_core::{Error, ErrorKind};
const OP: &str = "oai_pmh";
fn decode(message: &str) -> Error {
    Error::new(ErrorKind::Decode, OP, message)
}
pub(super) fn empty(field: &str) -> Error {
    Error::new(ErrorKind::InvalidInput, OP, format!("empty {field}"))
}
pub(super) fn response(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    decode("invalid OAI response").with_source(error)
}
pub(super) fn protocol(error: Option<ProtocolError>) -> Error {
    let result = decode("OAI protocol error or incomplete envelope");
    if let Some(error) = error {
        result.with_source(error)
    } else {
        result
    }
}
#[derive(Debug, serde::Deserialize)]
pub(super) struct ProtocolError {
    #[serde(rename = "@code")]
    pub(super) code: String,
}
impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OAI error: {}", self.code)
    }
}
impl std::error::Error for ProtocolError {}
