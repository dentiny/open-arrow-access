//! Indexed article lookups in an existing metadata DuckDB snapshot.
use super::{ArxivMetadataIndex, content::metadata_schema, error, id, validation};
use crate::duckdb::{DuckDbDatabase, params};
use arrow_array::{RecordBatch, StructArray, cast::AsArray};
use arrow_schema::DataType;
use open_arrow_access_core::Result;
use std::sync::Mutex;

/// Share one read-only DuckDB instance and its disk cache across article readers.
/// Construct it from `DuckDbDatabase::connect(location, secret_sql_file)` for
/// remote HTTP/S3 access, or `DuckDbDatabase::open(path)` for local snapshots.
pub struct ArxivMetadataDatabase {
    database: Mutex<DuckDbDatabase>,
}

impl ArxivMetadataDatabase {
    pub fn new(database: DuckDbDatabase) -> Self {
        Self {
            database: Mutex::new(database),
        }
    }

    pub(super) fn article(&self, article: &str) -> Result<ArxivMetadataIndex> {
        let article = id::metadata_id(article)?;
        let connection = self
            .database
            .lock()
            .map_err(|_| error::invalid("DuckDB metadata connection lock poisoned"))?
            .try_clone()?;
        let mut batches = Vec::new();
        // Match the indexed normalized key directly; do not transform the table
        // column or load the complete snapshot. Two rows also detect duplicates.
        let schema = connection.query(
            "SELECT \"oai:header\", \"oai:metadata\" FROM main.arxiv_metadata WHERE article_id = ? LIMIT 2",
            params![article],
            |batch| { batches.push(batch); Ok(()) },
        )?;
        let batch =
            arrow_select::concat::concat_batches(&schema, &batches).map_err(error::arrow_error)?;
        validation::validate_metadata_record_count(batch.num_rows())?;
        // Cast by field name to the exact conversion schema, dropping unsupported
        // OAI fields instead of emitting NULL placeholders for them.
        let metadata = arrow_cast::cast(
            &StructArray::from(batch),
            &DataType::Struct(metadata_schema().fields().clone()),
        )
        .map_err(error::arrow_error)?;
        let batch = RecordBatch::from(metadata.as_struct().clone());
        validation::validate_database_metadata(&batch, &article)?;
        ArxivMetadataIndex::from_content_records(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;

    #[test]
    fn looks_up_modern_and_legacy_articles_with_conversion_schema() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("arxiv.duckdb");
        let connection = ::duckdb::Connection::open(&path).unwrap();
        connection
            .execute_batch(include_str!("../../tests/arxiv_metadata.sql"))
            .unwrap();
        drop(connection);
        let database = ArxivMetadataDatabase::new(DuckDbDatabase::open(path).unwrap());
        for (article, title) in [
            ("2410.00001", "Modern"),
            ("oai:arXiv.org:hep-th/9901001", "Legacy"),
        ] {
            let index = database.article(article).unwrap();
            assert_eq!(index.rows.len(), 1);
            assert_eq!(index.rows.fields(), metadata_schema().fields());
            let raw = index.rows["oai:metadata"].as_struct()["arxiv:arXivRaw"].as_struct();
            assert_eq!(raw["arxiv:title"].as_string::<i32>().value(0), title);
        }
    }
}
