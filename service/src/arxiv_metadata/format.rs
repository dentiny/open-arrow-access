//! Typed arXivRaw mapping from https://oaipmh.arxiv.org/OAI/arXivRaw.xsd.
use std::sync::OnceLock;

use arrow_schema::DataType;
use open_arrow_access_file_format::xml::{NamespaceBinding, XmlField, XmlFormat, XmlSchema};

pub(super) fn record_format() -> XmlFormat {
    static SCHEMA: OnceLock<XmlSchema> = OnceLock::new();
    let schema = SCHEMA.get_or_init(|| {
        let header = XmlField::structure(
            "oai:header",
            "oai:header",
            false,
            vec![
                text("oai:identifier"),
                text("oai:datestamp"),
                text("@status"),
                XmlField::list("oai:setSpec", ".", true, text("oai:setSpec")),
            ],
        );
        let mut fields: Vec<_> = [
            "id",
            "submitter",
            "title",
            "authors",
            "categories",
            "comments",
            "proxy",
            "report-no",
            "acm-class",
            "msc-class",
            "journal-ref",
            "doi",
            "license",
            "abstract",
        ]
        .into_iter()
        .map(|name| text(&format!("arxiv:{name}")))
        .collect();
        fields.push(text("@xsi:schemaLocation"));
        fields.push(XmlField::list(
            "arxiv:version",
            ".",
            true,
            XmlField::structure(
                "item",
                "arxiv:version",
                false,
                vec![
                    text("@version"),
                    text("arxiv:date"),
                    text("arxiv:size"),
                    text("arxiv:source_type"),
                ],
            ),
        ));
        let metadata = XmlField::structure(
            "oai:metadata",
            "oai:metadata",
            true,
            vec![XmlField::structure(
                "arxiv:arXivRaw",
                "arxiv:arXivRaw",
                true,
                fields,
            )],
        );
        XmlSchema::with_namespaces(
            vec![header, metadata],
            &[
                NamespaceBinding {
                    prefix: "oai",
                    uri: "http://www.openarchives.org/OAI/2.0/",
                },
                NamespaceBinding {
                    prefix: "arxiv",
                    uri: "http://arxiv.org/OAI/arXivRaw/",
                },
                NamespaceBinding {
                    prefix: "xsi",
                    uri: "http://www.w3.org/2001/XMLSchema-instance",
                },
            ],
        )
        .expect("valid arXivRaw mapping")
    });
    XmlFormat::new("/oai:OAI-PMH/oai:ListRecords/oai:record").with_schema(schema.clone())
}

fn text(path: &str) -> XmlField {
    XmlField::new(path, path, DataType::Utf8, true)
}
