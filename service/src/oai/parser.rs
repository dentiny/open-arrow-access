use super::{OaiPage, error, validation};
use crate::xml_metadata::XmlDocument;
use arrow_array::{Array, cast::AsArray};
use arrow_schema::DataType::Utf8;
use open_arrow_access_core::{Bytes, Result};
use open_arrow_access_file_format::xml::XmlField;

pub(super) struct Envelope {
    pub(super) records: Option<Records>,
    pub(super) error: Option<error::ProtocolError>,
}

pub(super) struct Records {
    count: usize,
    token: Option<String>,
}

pub(super) async fn parse_page(bytes: Bytes) -> Result<OaiPage> {
    let document = XmlDocument::new(&bytes, "OAI-PMH")?;
    let batch = document
        .read(vec![
            XmlField::structure(
                "records",
                document.element("ListRecords"),
                true,
                vec![
                    XmlField::list(
                        "records",
                        ".",
                        false,
                        XmlField::structure("record", document.element("record"), false, vec![]),
                    ),
                    XmlField::new("token", document.element("resumptionToken"), Utf8, true),
                ],
            ),
            XmlField::structure(
                "error",
                document.element("error"),
                true,
                vec![XmlField::new("code", "@code", Utf8, false)],
            ),
        ])
        .await
        .map_err(error::response)?;
    let records = batch.column(0).as_struct();
    let failure = batch.column(1).as_struct();
    let envelope = Envelope {
        records: (!records.is_null(0)).then(|| Records {
            count: records.column(0).as_list::<i32>().value_length(0) as usize,
            token: records
                .column(1)
                .as_string::<i32>()
                .iter()
                .next()
                .flatten()
                .map(str::to_owned),
        }),
        error: (!failure.is_null(0)).then(|| error::ProtocolError {
            code: failure.column(0).as_string::<i32>().value(0).to_owned(),
        }),
    };
    validation::validate_response(&envelope)?;
    Ok(OaiPage {
        record_count: envelope.records.as_ref().map_or(0, |records| records.count),
        resumption_token: envelope
            .records
            .and_then(|records| records.token)
            .filter(|token| !token.is_empty()),
        bytes,
    })
}
