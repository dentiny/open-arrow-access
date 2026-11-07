use super::{decode, validation};
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
    validation::validate_pmcid_digits(digits, path)?;
    metadata.pmcid = format!("PMC{}", digits.trim_start_matches('0'));
    validation::validate_article_version(metadata.pmc_version, path)?;
    Ok(Some(metadata))
}

fn field<T: DeserializeOwned>(
    fields: &HashMap<String, &RawValue>,
    name: &str,
) -> serde_json::Result<T> {
    serde_json::from_str(fields.get(name).map_or("null", |value| value.get()))
}
