use open_arrow_access_core::{RecordBatch, RecordBatchBuilder, Result, SchemaRef};

use super::metadata::{Book, File};

pub(super) fn create_batch(
    book: &Book,
    raw: &[u8],
    selected: &[(&File, Vec<u8>)],
    schema: SchemaRef,
) -> Result<RecordBatch> {
    RecordBatchBuilder::build_row(super::schema(), schema, |row| {
        row.append("ebook_id", book.ebook_id)?;
        row.append("title", book.title.as_deref())?;
        row.append_structs("creator", &book.creator, |row, creator| {
            row.append("uri", creator.uri.as_deref())?;
            row.append("name", creator.name.as_deref())?;
            row.append_list("alias", creator.alias.iter().map(String::as_str))?;
            row.append("birthdate", creator.birthdate)?;
            row.append("deathdate", creator.deathdate)
        })?;
        row.append_list("language", book.language.iter().map(String::as_str))?;
        row.append_structs("subject", &book.subject, |row, subject| {
            row.append("scheme", subject.scheme.as_deref())?;
            row.append("value", subject.value.as_str())
        })?;
        row.append_list("bookshelf", book.bookshelf.iter().map(String::as_str))?;
        row.append("issued", book.issued)?;
        row.append("rights", book.rights.as_deref())?;
        row.append("metadata_rdf", raw)?;
        row.append_structs("files", selected, |row, (file, data)| {
            row.append("uri", file.uri.as_str())?;
            row.append_list("format", file.format.iter().map(String::as_str))?;
            row.append("extent", file.extent)?;
            row.append("modified", file.modified.as_deref())?;
            row.append("data", data.as_slice())
        })
    })
}
