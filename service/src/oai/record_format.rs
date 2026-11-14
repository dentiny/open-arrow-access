use std::sync::OnceLock;

use arrow_schema::DataType;
use open_arrow_access_file_format::xml::{NamespaceBinding, XmlField, XmlFormat, XmlSchema};

const RECORD_PATH: &str = "/oai:OAI-PMH/oai:ListRecords/oai:record";
const OAI_NAMESPACE: NamespaceBinding<'static> = NamespaceBinding {
    prefix: "oai",
    uri: "http://www.openarchives.org/OAI/2.0/",
};

pub(super) fn default_record_format() -> XmlFormat {
    static SCHEMA: OnceLock<XmlSchema> = OnceLock::new();
    let schema = SCHEMA.get_or_init(|| {
        XmlSchema::with_namespaces(
            vec![
                XmlField::new(
                    "identifier",
                    "oai:header/oai:identifier",
                    DataType::Utf8,
                    false,
                ),
                XmlField::new(
                    "datestamp",
                    "oai:header/oai:datestamp",
                    DataType::Utf8,
                    true,
                ),
                XmlField::new("status", "oai:header/@status", DataType::Utf8, true),
                XmlField::list(
                    "set_specs",
                    "oai:header",
                    false,
                    XmlField::new("item", "oai:setSpec", DataType::Utf8, false),
                ),
            ],
            &[OAI_NAMESPACE],
        )
        .expect("valid OAI-PMH header mapping")
    });
    XmlFormat::new(RECORD_PATH).with_schema(schema.clone())
}
