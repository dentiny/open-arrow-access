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
            || pieces[1].split_once('-') != Some((fields[0], fields[1]))
        {
            return Err(super::invalid(format!("invalid OCR page identity: {key}")));
        }
        Ok(Self {
            entity_type: fields[0].to_owned(),
            entity_id: number(fields[1])?,
            page_id: number(fields[2])?,
            sequence: nonnegative_number(fields[3])?,
        })
    }
}

pub(in crate::bhl) fn number(value: &str) -> Result<u64> {
    let number = nonnegative_number(value)?;
    if number == 0 {
        return Err(super::invalid(format!("invalid BHL numeric ID: {value:?}")));
    }
    Ok(number)
}

fn nonnegative_number(value: &str) -> Result<u64> {
    (!value.is_empty() && value.bytes().all(|c| c.is_ascii_digit()))
        .then(|| value.parse::<u64>().ok())
        .flatten()
        .ok_or_else(|| super::invalid(format!("invalid BHL number: {value:?}")))
}
