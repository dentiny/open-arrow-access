use super::{error, validation};
use open_arrow_access_core::{Bytes, ReadRequest, RecordBatch, Result};
use open_arrow_access_file_format::{
    read_document,
    xml::{NamespaceBinding, XmlField, XmlFormat, XmlLimits, XmlSchema},
};
use quick_xml::{NsReader, events::Event, name::ResolveResult};
use std::borrow::Cow;

/// An XML metadata document whose element mappings use its root namespace.
/// External DTD declarations are ignored; internal subsets are rejected.
/// No DTD or external entity is fetched.
pub struct XmlDocument<'a> {
    bytes: Cow<'a, [u8]>,
    root: String,
    namespace: Option<String>,
}

impl<'a> XmlDocument<'a> {
    pub fn new(bytes: &'a [u8], expected_root: &str) -> Result<Self> {
        let mut reader = NsReader::from_reader(bytes);
        let mut doctype = None;
        loop {
            let start = reader.buffer_position() as usize;
            let (namespace, event) = reader.read_resolved_event().map_err(error::xml)?;
            match event {
                Event::Start(element) | Event::Empty(element) => {
                    validation::validate_root_name(
                        std::str::from_utf8(element.local_name().as_ref()).map_err(error::xml)?,
                        expected_root,
                    )?;
                    let namespace = match namespace {
                        ResolveResult::Bound(namespace) => Some(
                            std::str::from_utf8(namespace.as_ref())
                                .map_err(error::xml)?
                                .to_owned(),
                        ),
                        ResolveResult::Unbound => None,
                        ResolveResult::Unknown(_) => {
                            return Err(error::invalid("unbound XML prefix"));
                        }
                    };
                    let bytes = match doctype {
                        Some((start, end)) => {
                            let mut normalized = bytes[..start].to_vec();
                            normalized.extend_from_slice(&bytes[end..]);
                            Cow::Owned(normalized)
                        }
                        None => Cow::Borrowed(bytes),
                    };
                    return Ok(Self {
                        bytes,
                        root: expected_root.into(),
                        namespace,
                    });
                }
                Event::DocType(value) => {
                    validation::validate_doctype(
                        std::str::from_utf8(value.as_ref()).map_err(error::xml)?,
                        doctype.is_some(),
                    )?;
                    doctype = Some((start, reader.buffer_position() as usize));
                }
                Event::Decl(_) | Event::Comment(_) | Event::PI(_) => {}
                Event::Text(text) if text.trim_ascii().is_empty() => {}
                _ => return Err(error::root(expected_root)),
            }
        }
    }

    /// Qualify one element name for this document's namespace. Attribute paths
    /// remain unprefixed unless the attribute itself has a namespace.
    pub fn element(&self, local_name: &str) -> String {
        if self.namespace.is_some() {
            format!("m:{local_name}")
        } else {
            local_name.into()
        }
    }

    /// Read one root record using our XML engine and validate the document tail.
    pub async fn read(&self, fields: Vec<XmlField>) -> Result<RecordBatch> {
        let bindings = self
            .namespace
            .as_deref()
            .map(|uri| NamespaceBinding { prefix: "m", uri });
        let schema = XmlSchema::with_namespaces(fields, bindings.as_slice()).map_err(error::xml)?;
        // Allow buffered metadata plus namespace overhead; retain the default depth limit.
        let budget = self.bytes.len().saturating_add(4096);
        let format = XmlFormat::new(format!("/{}", self.element(&self.root)))
            .with_schema(schema)
            .with_limits(XmlLimits {
                max_record_bytes: budget,
                max_batch_bytes: budget,
                ..Default::default()
            });
        read_document(
            Bytes::copy_from_slice(&self.bytes),
            format,
            ReadRequest::new("xml-metadata"),
        )
        .await
    }
}

/// Check the document root without fetching a DTD or decoding record values.
pub fn validate_root(bytes: &[u8], expected: &str) -> Result<()> {
    XmlDocument::new(bytes, expected).map(|_| ())
}
