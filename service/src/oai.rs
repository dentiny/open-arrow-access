//! Shared OAI-PMH ListRecords protocol. Pages retain exact upstream XML.
use crate::remote::{Connection, decode, invalid};
use open_arrow_access_core::{ByteMetadata, ByteReader, Bytes, Result, StreamReader};
use quick_xml::{Reader, events::Event};

/// One page. Deleted records and all provider metadata remain in `bytes`.
#[derive(Clone, Debug)]
pub struct OaiPage {
    pub bytes: Bytes,
    pub resumption_token: Option<String>,
    pub record_count: usize,
}
impl OaiPage {
    pub fn reader(&self) -> Box<dyn ByteReader> {
        Box::new(StreamReader::new(
            futures::stream::iter([Ok(self.bytes.clone())]),
            ByteMetadata {
                source: None,
                content_length: Some(self.bytes.len() as u64),
                version: None,
            },
        ))
    }
}
/// Protocol shared by arXiv, DOAB and OAPEN. One method call fetches one page.
#[derive(Clone, Debug)]
pub struct OaiService {
    connection: Connection,
    path: String,
    metadata_prefix: String,
}
impl OaiService {
    pub fn new(connection: Connection, path: &str, metadata_prefix: &str) -> Result<Self> {
        connection.url(path, &[])?;
        if metadata_prefix.is_empty() {
            return Err(invalid("empty metadata prefix"));
        }
        Ok(Self {
            connection,
            path: path.into(),
            metadata_prefix: metadata_prefix.into(),
        })
    }
    pub async fn records(
        &self,
        from: Option<&str>,
        until: Option<&str>,
        set: Option<&str>,
    ) -> Result<OaiPage> {
        let mut query = vec![
            ("verb", "ListRecords"),
            ("metadataPrefix", &self.metadata_prefix),
        ];
        for (key, value) in [("from", from), ("until", until), ("set", set)] {
            if let Some(value) = value {
                if value.is_empty() {
                    return Err(invalid("empty OAI selector"));
                }
                query.push((key, value));
            }
        }
        self.page(&query).await
    }
    /// Resumption requests contain only verb and token, as required by OAI-PMH.
    pub async fn resume(&self, token: &str) -> Result<OaiPage> {
        if token.is_empty() {
            return Err(invalid("empty resumption token"));
        }
        self.page(&[("verb", "ListRecords"), ("resumptionToken", token)])
            .await
    }
    async fn page(&self, query: &[(&str, &str)]) -> Result<OaiPage> {
        let url = self.connection.url(&self.path, query)?;
        let bytes = self.connection.bytes(url, "application/xml").await?;
        parse(bytes)
    }
}
#[derive(serde::Deserialize)]
struct Envelope {
    #[serde(rename = "ListRecords")]
    records: Option<Records>,
    error: Option<ProtocolError>,
}
#[derive(serde::Deserialize)]
struct Records {
    #[serde(rename = "record", default)]
    records: Vec<serde::de::IgnoredAny>,
    #[serde(rename = "resumptionToken")]
    token: Option<String>,
}
#[derive(serde::Deserialize)]
struct ProtocolError {
    #[serde(rename = "@code")]
    code: String,
}
fn parse(bytes: Bytes) -> Result<OaiPage> {
    // serde decodes known fields and ignores provider-owned metadata. Check the
    // envelope name first so an HTML/error response cannot masquerade as OAI.
    let mut reader = Reader::from_reader(bytes.as_ref());
    loop {
        match reader
            .read_event()
            .map_err(|e| decode("invalid OAI XML").with_source(e))?
        {
            Event::Start(e) if e.local_name().as_ref() == b"OAI-PMH" => break,
            Event::Decl(_) | Event::Comment(_) => {}
            Event::Text(e) if e.iter().all(u8::is_ascii_whitespace) => {}
            _ => return Err(decode("expected OAI-PMH envelope")),
        }
    }
    let envelope: Envelope = quick_xml::de::from_reader(bytes.as_ref())
        .map_err(|e| decode("invalid OAI response").with_source(e))?;
    let (record_count, resumption_token) = match (envelope.records, envelope.error) {
        (Some(records), None) => (
            records.records.len(),
            records.token.filter(|s| !s.is_empty()),
        ),
        (None, Some(error)) if error.code == "noRecordsMatch" => (0, None),
        _ => {
            return Err(decode(
                "OAI provider returned a protocol error or incomplete envelope",
            ));
        }
    };
    Ok(OaiPage {
        bytes,
        resumption_token,
        record_count,
    })
}
