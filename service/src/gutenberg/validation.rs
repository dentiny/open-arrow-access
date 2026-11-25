use super::{Mapping, error};
use crate::arrow_utils::string_value;
use arrow_array::{Array, cast::AsArray};
use arrow_schema::DataType;
use chrono::NaiveDate;
use open_arrow_access_core::{FieldValue, Result};

pub(super) fn mirror_path(uri: &str) -> Result<String> {
    let invalid = || error::invalid_mirror_uri(uri);
    let url = url::Url::parse(uri).map_err(|_| invalid())?;
    if !matches!(url.scheme(), "http" | "https")
        || !matches!(url.host_str(), Some("www.gutenberg.org" | "gutenberg.org"))
        || url.port().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(invalid());
    }
    let decoded = percent_encoding::percent_decode_str(url.path())
        .decode_utf8()
        .map_err(|_| invalid())?;
    let path = decoded.trim_start_matches('/');
    if path.split('/').any(|part| matches!(part, "." | "..")) || path.contains(['\\', '\0']) {
        return Err(invalid());
    }
    if let Some(rest) = path.strip_prefix("files/") {
        let (id, member) = rest.split_once('/').ok_or_else(invalid)?;
        if id.is_empty() || !id.bytes().all(|b| b.is_ascii_digit()) {
            return Err(invalid());
        }
        let mut digits = String::new();
        if id.len() == 1 {
            digits.push_str("0/");
        } else {
            for digit in id[..id.len() - 1].chars() {
                digits.push(digit);
                digits.push('/');
            }
        }
        return Ok(format!("{digits}{id}/{member}"));
    }
    if let Some(rest) = path.strip_prefix("ebooks/") {
        let (id, format) = rest.split_once('.').ok_or_else(invalid)?;
        if id.is_empty() || !id.bytes().all(|b| b.is_ascii_digit()) {
            return Err(invalid());
        }
        let suffix = match format {
            "html.images" => "-images.html",
            "html.noimages" => ".html",
            "epub3.images" => "-images-3.epub",
            "epub3.noimages" => "-3.epub",
            "epub.images" => "-images.epub",
            "epub.noimages" => ".epub",
            "kf8.images" => "-images-kf8.mobi",
            "kf8.noimages" => "-kf8.mobi",
            "kindle.images" => "-images.mobi",
            "kindle.noimages" => ".mobi",
            "txt.utf-8" => ".txt",
            "rdf" => ".rdf",
            _ => {
                return Err(invalid());
            }
        };
        return Ok(format!("cache/epub/{id}/pg{id}{suffix}"));
    }
    if let Some(rest) = path.strip_prefix("cache/generated/") {
        return Ok(format!("cache/epub/{rest}"));
    }
    if path.starts_with("cache/epub/") {
        return Ok(path.to_owned());
    }
    Err(invalid())
}

pub(super) fn scalar_value<'a>(
    mapping: &Mapping,
    column: &'a dyn Array,
    index: usize,
) -> Result<FieldValue<'a>> {
    let field = &mapping.field;
    let name = field.name().as_str();
    // Empty attributes are values; empty optional element text becomes null.
    let keep_empty = matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8)
        && mapping.xml.path().contains('@');
    let text =
        string_value(column.as_string::<i32>(), index).filter(|s| !s.is_empty() || keep_empty);
    if !field.is_nullable() && text.is_none_or(str::is_empty) {
        return Err(error::missing_field(name));
    }
    if let Some(parse) = mapping.parser {
        return Ok(text.map(parse).transpose()?.unwrap_or(FieldValue::Null));
    }
    Ok(match field.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => text.into(),
        DataType::UInt64 => text
            .map(|text| number::<u64>(text, name))
            .transpose()?
            .into(),
        DataType::Int32 => text
            .map(|text| number::<i32>(text, name))
            .transpose()?
            .into(),
        DataType::Date32 => text.map(parse_issued_date).transpose()?.into(),
        _ => unreachable!("unsupported Gutenberg metadata type"),
    })
}

pub(super) fn number<T: std::str::FromStr>(value: &str, field: &str) -> Result<T> {
    value
        .parse()
        .map_err(|_| error::invalid_number(field, value))
}

pub(super) fn parse_ebook_id(about: &str) -> Result<FieldValue<'_>> {
    let id = about
        .strip_prefix("ebooks/")
        .or_else(|| about.strip_prefix("http://www.gutenberg.org/ebooks/"))
        .or_else(|| about.strip_prefix("https://www.gutenberg.org/ebooks/"))
        .ok_or_else(error::invalid_ebook_uri)?;
    id.parse::<u64>()
        .ok()
        .filter(|id| *id != 0)
        .filter(|_| id.bytes().all(|byte| byte.is_ascii_digit()))
        .map(FieldValue::UInt64)
        .ok_or_else(error::invalid_ebook_id)
}

fn parse_issued_date(value: &str) -> Result<i32> {
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(error::decode_source)?;
    Ok(date
        .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32)
}

#[cfg(test)]
mod tests {
    #[test]
    fn maps_gutenberg_aliases_to_mirror_paths() {
        for (uri, expected) in [
            ("https://www.gutenberg.org/files/1/1-0.txt", "0/1/1-0.txt"),
            (
                "https://www.gutenberg.org/files/12345/a%20b.txt",
                "1/2/3/4/12345/a b.txt",
            ),
            (
                "https://www.gutenberg.org/ebooks/42.epub.images",
                "cache/epub/42/pg42-images.epub",
            ),
            (
                "https://www.gutenberg.org/ebooks/42.html.noimages",
                "cache/epub/42/pg42.html",
            ),
            (
                "http://www.gutenberg.org/cache/generated/42/pg42.txt",
                "cache/epub/42/pg42.txt",
            ),
        ] {
            assert_eq!(super::mirror_path(uri).unwrap(), expected);
        }
        for uri in [
            "https://example.org/files/1/1.txt",
            "https://www.gutenberg.org/files/1/a%5Cb",
            "https://www.gutenberg.org/files/1/a?token=x",
            "https://u:p@www.gutenberg.org/files/1/a",
            "https://www.gutenberg.org:8443/files/1/a",
            "https://www.gutenberg.org/files/1/../../private",
            "https://www.gutenberg.org/ebooks/42.unknown",
        ] {
            assert!(super::mirror_path(uri).is_err());
        }
    }
}
