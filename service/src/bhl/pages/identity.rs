use open_arrow_access_core::Result;

/// Numeric join keys; original zero padding remains in `source_path`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BhlPageIdentity {
    pub entity_type: String,
    pub entity_id: u64,
    pub page_id: u64,
    pub sequence: u64,
}

impl BhlPageIdentity {
    pub fn parse(key: &str) -> Result<Self> {
        let pieces = key.split('/').collect::<Vec<_>>();
        if pieces.len() != 3 || pieces[0] != "ocr" {
            return Err(super::invalid(format!("invalid OCR page key: {key}")));
        }
        let name = pieces[2].strip_suffix(".txt").unwrap_or("");
        let fields = name.split('-').collect::<Vec<_>>();
        if fields.len() != 4
            || !matches!(fields[0], "item" | "part")
            || pieces[1] != format!("{}-{}", fields[0], fields[1])
        {
            return Err(super::invalid(format!("invalid OCR page identity: {key}")));
        }
        Ok(Self {
            entity_type: fields[0].to_owned(),
            entity_id: field_number(fields[1], "entity_id", key, true)?,
            page_id: field_number(fields[2], "page_id", key, true)?,
            sequence: field_number(fields[3], "sequence", key, false)?,
        })
    }
}

pub(in crate::bhl) fn number(value: &str) -> Result<u64> {
    if value.is_empty() || !value.bytes().all(|c| c.is_ascii_digit()) {
        return Err(super::invalid(format!(
            "invalid BHL numeric ID/sequence: {value:?}"
        )));
    }
    value
        .parse::<u64>()
        .ok()
        .filter(|&n| n > 0)
        .ok_or_else(|| super::invalid(format!("invalid BHL numeric ID/sequence: {value:?}")))
}

fn field_number(value: &str, field: &str, key: &str, positive: bool) -> Result<u64> {
    let parsed = (!value.is_empty() && value.bytes().all(|c| c.is_ascii_digit()))
        .then(|| value.parse::<u64>().ok())
        .flatten()
        .filter(|&n| !positive || n > 0);
    parsed.ok_or_else(|| super::invalid(format!("invalid BHL {field} {value:?} in {key}")))
}
