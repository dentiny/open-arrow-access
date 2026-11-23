use super::{ArxivMetadataIndex, content::content_schema, error, id, validation};
use crate::duckdb::{DuckDbConnectionPool, DuckDbDatabase, params};
use arrow_array::{RecordBatch, StructArray, cast::AsArray};
use open_arrow_access_core::Result;

/// Share one read-only DuckDB instance and its disk cache across article readers.
pub struct ArxivMetadataDatabase {
    connections: DuckDbConnectionPool,
}

impl ArxivMetadataDatabase {
    pub fn new(database: DuckDbDatabase) -> Self {
        Self {
            connections: DuckDbConnectionPool::new(database),
        }
    }

    pub(super) fn article(&self, article: &str) -> Result<ArxivMetadataIndex> {
        let article = id::metadata_id(article)?;
        let connection = self.connections.get_connection()?;
        let mut batches = Vec::new();
        // LIMIT 2 lets validation reject duplicate article IDs.
        let schema = connection.query_cached(
            "SELECT \"oai:header\", \"oai:metadata\" FROM main.arxiv_metadata WHERE article_id = ? LIMIT 2",
            params![article],
            |batch| { batches.push(batch); Ok(()) },
        )?;
        drop(connection);
        let batch =
            arrow_select::concat::concat_batches(&schema, &batches).map_err(error::arrow_error)?;
        validation::validate_metadata_record_count(batch.num_rows())?;
        let metadata = arrow_cast::cast(
            &StructArray::from(batch),
            content_schema()
                .field_with_name("metadata")
                .unwrap()
                .data_type(),
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

    fn database() -> (tempfile::TempDir, ArxivMetadataDatabase) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("arxiv.duckdb");
        let connection = ::duckdb::Connection::open(&path).unwrap();
        connection
            .execute_batch(include_str!("../../tests/arxiv_metadata.sql"))
            .unwrap();
        drop(connection);
        let database = ArxivMetadataDatabase::new(DuckDbDatabase::open(path).unwrap());
        (directory, database)
    }

    #[test]
    fn looks_up_modern_and_legacy_articles_with_conversion_schema() {
        let (_directory, database) = database();
        for (article, title) in [
            ("2410.00001", "Modern"),
            ("oai:arXiv.org:hep-th/9901001", "Legacy"),
            ("2410.00001", "Modern"),
        ] {
            let index = database.article(article).unwrap();
            assert_eq!(index.rows.len(), 1);
            assert_eq!(
                index.rows.data_type(),
                content_schema()
                    .field_with_name("metadata")
                    .unwrap()
                    .data_type()
            );
            let raw = index.rows["oai:metadata"].as_struct()["arxiv:arXivRaw"].as_struct();
            assert_eq!(raw["arxiv:title"].as_string::<i32>().value(0), title);
        }
        // An empty result must not leave the cached statement's old row behind.
        assert!(database.article("2410.99999").is_err());
        assert!(database.article("2410.00001").is_ok());
    }
}
