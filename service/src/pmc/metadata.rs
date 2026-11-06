use super::{decode, valid_path};
use open_arrow_access_core::Result;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::value::RawValue;
use std::collections::HashMap;

/// Provider mapping only. Arrow construction is delegated to JsonAccessor.
#[derive(Serialize)]
pub(super) struct Metadata {
    pmcid: String,
    pmc_version: Option<u32>,
    pmid: Option<String>,
    doi: Option<String>,
    title: Option<String>,
    license: Option<String>,
    pub source_uri: String,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Pmid {
    Text(String),
    Number(u64),
}

pub(super) fn parse(raw: &[u8], path: &str) -> Result<Option<Metadata>> {
    // RawValue preserves unknown values (including numbers outside f64's range)
    // and accepts exactly one complete object. Duplicate keys use the last value.
    let parsed = (|| -> serde_json::Result<Option<Metadata>> {
        let fields: HashMap<String, &RawValue> = serde_json::from_slice(raw)?;
        let Some(source_uri) = field::<Option<String>>(&fields, "pdf_url")? else {
            return Ok(None);
        };
        Ok(Some(Metadata {
            pmcid: field(&fields, "pmcid")?,
            pmc_version: field(&fields, "version")?,
            pmid: field::<Option<Pmid>>(&fields, "pmid")?.map(|pmid| match pmid {
                Pmid::Text(text) => text,
                Pmid::Number(number) => number.to_string(),
            }),
            doi: field(&fields, "doi")?,
            title: field(&fields, "title")?,
            license: field(&fields, "license_code")?,
            source_uri,
        }))
    })()
    .map_err(|error| decode(format!("{path}: {error}")).with_source(error))?;
    let Some(mut metadata) = parsed else {
        return Ok(None);
    };
    let pmcid = metadata.pmcid.trim();
    let digits = if pmcid
        .get(..3)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("PMC"))
    {
        &pmcid[3..]
    } else {
        pmcid
    };
    if digits.is_empty()
        || !digits.bytes().all(|b| b.is_ascii_digit())
        || digits.bytes().all(|b| b == b'0')
    {
        return Err(decode(format!("{path}: invalid PMCID")));
    }
    metadata.pmcid = format!("PMC{}", digits.trim_start_matches('0'));
    if metadata.pmc_version == Some(0) {
        return Err(decode(format!("{path}: article version must be positive")));
    }
    Ok(Some(metadata))
}

fn field<T: DeserializeOwned>(
    fields: &HashMap<String, &RawValue>,
    name: &str,
) -> serde_json::Result<T> {
    serde_json::from_str(fields.get(name).map_or("null", |value| value.get()))
}

pub(super) fn pdf_path(uri: &str) -> Result<String> {
    let invalid = || decode(format!("invalid PMC PDF URL: {uri}"));
    let path = uri
        .strip_prefix("s3://pmc-oa-opendata/")
        .ok_or_else(invalid)?;
    let path = path.split(['?', '#']).next().unwrap_or_default();
    // Decode exactly once; '+' is a literal filename character.
    let path = percent_encoding::percent_decode_str(path)
        .decode_utf8()
        .map_err(|_| invalid())?;
    if !valid_path(&path) {
        return Err(invalid());
    }
    Ok(path.into_owned())
}

pub(super) fn validate_pdf(bytes: &[u8], path: &str) -> Result<()> {
    // Header validation only, not a PDF parser or completeness check.
    if bytes.len() < 9
        || !bytes.starts_with(b"%PDF-")
        || !bytes[5].is_ascii_digit()
        || bytes[6] != b'.'
        || !bytes[7].is_ascii_digit()
        || !matches!(bytes[8], b'\r' | b'\n')
    {
        return Err(decode(format!("{path}: invalid PDF header")));
    }
    Ok(())
}
