use open_arrow_access_core::Result;

pub(crate) fn parse_json<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    operation: &'static str,
) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|source| super::json_error(operation, source))
}
