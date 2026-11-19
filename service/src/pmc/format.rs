//! Choose one article representation for downstream content ingestion.
use open_arrow_access_core::{Error, Result};
use std::{fmt, str::FromStr};

/// Content URL to select. Other representations remain available in metadata.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PmcFormat {
    #[default]
    Pdf,
    Xml,
    Txt,
}
impl PmcFormat {
    pub(super) fn metadata_field(self) -> &'static str {
        match self {
            Self::Pdf => "pdf_url",
            Self::Xml => "xml_url",
            Self::Txt => "text_url",
        }
    }
}
impl fmt::Display for PmcFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Pdf => "pdf",
            Self::Xml => "xml",
            Self::Txt => "txt",
        })
    }
}
impl FromStr for PmcFormat {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self> {
        match value {
            "pdf" => Ok(Self::Pdf),
            "xml" => Ok(Self::Xml),
            "txt" => Ok(Self::Txt),
            _ => Err(super::error::invalid_format()),
        }
    }
}
