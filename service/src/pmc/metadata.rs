use super::{decode, validation};
use open_arrow_access_core::Result;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::value::RawValue;
use std::collections::HashMap;

#[derive(Serialize)]
pub(super) struct Metadata {
    pmcid: String,
    version: Option<u32>,
    pmid: Option<String>,
    doi: Option<String>,
    title: Option<String>,
    citation: Option<String>,
    is_pmc_openaccess: Option<bool>,
    is_manuscript: Option<bool>,
    is_historical_ocr: Option<bool>,
    is_retracted: Option<bool>,
    license_code: Option<String>,
    xml_url: Option<String>,
    pdf_url: Option<String>,
    media_urls: Option<Vec<String>>,
    text_url: Option<String>,
}
#[derive(Deserialize)]
#[serde(untagged)]
enum Pmid {
    Text(String),
    Number(u64),
}

pub(super) fn parse(raw: &[u8], path: &str) -> Result<Metadata> {
    let parsed = (|| -> serde_json::Result<Metadata> {
        // Exactly one object; unknown values and duplicate keys survive in raw JSON.
        let fields: HashMap<String, &RawValue> = serde_json::from_slice(raw)?;
        Ok(Metadata {
            pmcid: field(&fields, "pmcid")?,
            version: field(&fields, "version")?,
            pmid: field::<Option<Pmid>>(&fields, "pmid")?.map(|value| match value {
                Pmid::Text(text) => text,
                Pmid::Number(number) => number.to_string(),
            }),
            doi: field(&fields, "doi")?,
            title: field(&fields, "title")?,
            citation: field(&fields, "citation")?,
            is_pmc_openaccess: field(&fields, "is_pmc_openaccess")?,
            is_manuscript: field(&fields, "is_manuscript")?,
            is_historical_ocr: field(&fields, "is_historical_ocr")?,
            is_retracted: field(&fields, "is_retracted")?,
            license_code: field(&fields, "license_code")?,
            xml_url: field(&fields, "xml_url")?,
            pdf_url: field(&fields, "pdf_url")?,
            media_urls: field(&fields, "media_urls")?,
            text_url: field(&fields, "text_url")?,
        })
    })()
    .map_err(|error| decode(format!("{path}: {error}")).with_source(error))?;
    validation::validate_pmcid(&parsed.pmcid, path)?;
    validation::validate_article_version(parsed.version, path)?;
    Ok(parsed)
}
fn field<T: DeserializeOwned>(
    fields: &HashMap<String, &RawValue>,
    name: &str,
) -> serde_json::Result<T> {
    serde_json::from_str(fields.get(name).map_or("null", |value| value.get()))
}
