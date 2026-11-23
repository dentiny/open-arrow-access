use arrow_array::{RecordBatch, cast::AsArray, types::UInt64Type};
use open_arrow_access_core::{Error, ErrorKind, ObjectRef};

pub(super) fn invalid(message: &str) -> Error {
    Error::new(ErrorKind::InvalidInput, "configure_arxiv", message)
}

pub(super) fn decode(message: &str) -> Error {
    Error::new(ErrorKind::Decode, "resolve_arxiv", message)
}

pub(super) fn xml_error(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    decode("invalid arXiv bulk manifest XML").with_source(error)
}

pub(super) fn arrow_error(error: arrow_schema::ArrowError) -> Error {
    decode("invalid arXiv metadata batch").with_source(error)
}

pub(super) fn metadata_ids_not_strings() -> Error {
    invalid("metadata IDs must be strings")
}

pub(super) fn null_metadata_id() -> Error {
    invalid("metadata ID cannot be null")
}

pub(super) fn invalid_month_encoding() -> Error {
    invalid("arXiv month must be encoded as YYMM")
}

pub(super) fn invalid_month_number() -> Error {
    invalid("arXiv month must have MM between 01 and 12")
}

pub(super) fn invalid_archive_month() -> Error {
    decode("invalid archive month")
}

pub(super) fn invalid_archive_kind() -> Error {
    decode("archive kind must be Pdf or Source")
}

pub(super) fn invalid_archive_filename() -> Error {
    decode("archive filename does not match kind, month and sequence")
}

pub(super) fn invalid_archive_checksum() -> Error {
    decode("invalid archive MD5 checksum")
}

pub(super) fn archive_length_mismatch() -> Error {
    decode("archive length is smaller than manifest payload size")
}

pub(super) fn non_ascii_metadata_id() -> Error {
    invalid("expected an ASCII arXiv article ID")
}

pub(super) fn invalid_metadata_id() -> Error {
    invalid("expected an arXiv article ID")
}

pub(super) fn invalid_unversioned_metadata_id() -> Error {
    invalid("expected an unversioned arXiv article ID")
}

pub(super) fn invalid_pdf_entry_encoding() -> Error {
    decode("PDF entry name is not UTF-8")
}

pub(super) fn invalid_pdf_extension() -> Error {
    decode("expected a PDF entry")
}

pub(super) fn invalid_pdf_id() -> Error {
    decode("PDF entry has an invalid arXiv ID")
}

pub(super) fn missing_metadata_id_column() -> Error {
    invalid("metadata ID column is missing")
}

pub(super) fn duplicate_metadata_id() -> Error {
    invalid("duplicate arXiv metadata ID")
}

pub(super) fn missing_pdf_entry_column() -> Error {
    decode("PDF entry_name column is missing")
}

pub(super) fn null_pdf_entry_name() -> Error {
    decode("null PDF entry name")
}

pub(super) fn missing_pdf_metadata() -> Error {
    decode("PDF has no matching metadata in the snapshot")
}

pub(super) fn pdf_kind_required() -> Error {
    invalid("metadata enrichment requires kind=Pdf")
}

pub(super) fn invalid_manifest_kind() -> Error {
    decode("manifest kind must be Pdf or Source")
}

pub(super) fn duplicate_archive() -> Error {
    decode("duplicate archive in arXiv manifest")
}

pub(super) fn invalid_shard() -> Error {
    invalid("shard must be a relative TAR key without a month selector")
}

pub(super) fn invalid_oai_metadata_schema() -> Error {
    invalid("expected the complete typed arXivRaw OAI schema")
}

pub(super) fn invalid_content_metadata_schema() -> Error {
    invalid("content metadata requires the typed OAI or conversion metadata schema")
}

pub(super) fn missing_content_metadata(id: &str) -> Error {
    decode(&format!(
        "article {id} has no matching metadata in the snapshot"
    ))
}

pub(super) fn invalid_metadata_response() -> Error {
    decode("expected one metadata record matching the requested arXiv article")
}

/// Missing representation, distinct from corrupt source bytes.
pub(super) fn missing_latex(batch: &RecordBatch, article: &str) -> Error {
    let shard = batch["shard_uri"].as_string::<i64>().value(0);
    let entry_index = batch["entry_index"].as_primitive::<UInt64Type>().value(0);
    Error::new(
        ErrorKind::NotFound,
        "read_arxiv_latex",
        format!("LaTeX content is unavailable for arXiv article {article}"),
    )
    .with_object(Some(&ObjectRef::new(format!(
        "{shard}#entry={entry_index}"
    ))))
}

pub(super) fn metadata_worker(source: tokio::task::JoinError) -> Error {
    Error::new(
        ErrorKind::Internal,
        "arxiv_metadata_worker",
        source.to_string(),
    )
    .with_source(source)
}
