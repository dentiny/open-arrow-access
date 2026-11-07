use super::{decode, validation};
use arrow_array::{
    ArrayRef, BooleanArray, RecordBatchOptions, UInt32Array,
    builder::{LargeStringBuilder, ListBuilder, StringBuilder},
};
use open_arrow_access_core::{RecordBatch, Result, SchemaRef};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::value::RawValue;
use std::{collections::HashMap, sync::Arc};

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

impl Metadata {
    pub fn into_batch(self, raw: &str, schema: SchemaRef) -> Result<RecordBatch> {
        let columns = schema
            .fields()
            .iter()
            .map(|field| -> ArrayRef {
                match field.name().as_str() {
                    "pmcid" => string(Some(&self.pmcid)),
                    "version" => Arc::new(UInt32Array::from(vec![self.version])),
                    "pmid" => string(self.pmid.as_deref()),
                    "doi" => string(self.doi.as_deref()),
                    "title" => large_string(self.title.as_deref()),
                    "citation" => large_string(self.citation.as_deref()),
                    "is_pmc_openaccess" => {
                        Arc::new(BooleanArray::from(vec![self.is_pmc_openaccess]))
                    }
                    "is_manuscript" => Arc::new(BooleanArray::from(vec![self.is_manuscript])),
                    "is_historical_ocr" => {
                        Arc::new(BooleanArray::from(vec![self.is_historical_ocr]))
                    }
                    "is_retracted" => Arc::new(BooleanArray::from(vec![self.is_retracted])),
                    "license_code" => string(self.license_code.as_deref()),
                    "xml_url" => string(self.xml_url.as_deref()),
                    "pdf_url" => string(self.pdf_url.as_deref()),
                    "media_urls" => {
                        let urls = self.media_urls.as_deref().unwrap_or_default();
                        let mut builder = ListBuilder::with_capacity(
                            StringBuilder::with_capacity(
                                urls.len(),
                                urls.iter().map(String::len).sum(),
                            ),
                            1,
                        );
                        for url in urls {
                            builder.values().append_value(url);
                        }
                        builder.append(self.media_urls.is_some());
                        Arc::new(builder.finish())
                    }
                    "text_url" => string(self.text_url.as_deref()),
                    "raw_metadata_json" => large_string(Some(raw)),
                    _ => unreachable!("projection is resolved against the PMC schema"),
                }
            })
            .collect();
        RecordBatch::try_new_with_options(
            schema,
            columns,
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .map_err(|error| decode(error.to_string()).with_source(error))
    }
}

// Arrow's FromIterator reserves 1 KiB per optional string column. Size these
// single-row buffers from the value, including zero bytes for null columns.
fn string(value: Option<&str>) -> ArrayRef {
    let mut builder = StringBuilder::with_capacity(1, value.map_or(0, str::len));
    builder.append_option(value);
    Arc::new(builder.finish())
}

fn large_string(value: Option<&str>) -> ArrayRef {
    let mut builder = LargeStringBuilder::with_capacity(1, value.map_or(0, str::len));
    builder.append_option(value);
    Arc::new(builder.finish())
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
