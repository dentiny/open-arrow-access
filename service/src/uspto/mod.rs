//! USPTO Open Data Portal bulk products, paginated file lists and original downloads.
mod error;
mod parser;
mod validation;
use crate::common::{ApiDocument, ApiKey, Connection, RemoteFiles, filename};
use open_arrow_access_core::Result;
#[derive(Clone, Debug)]
pub struct UsptoService {
    connection: Connection,
}
impl UsptoService {
    pub fn new(key: &str) -> Result<Self> {
        Self::with_connection(Connection::new("https://api.uspto.gov")?).with_key(key)
    }

    /// Set or replace the API key while preserving the configured connection.
    /// The key is validated locally and sent as `X-API-KEY` only within its origin.
    pub fn with_key(mut self, key: &str) -> Result<Self> {
        self.connection = self.connection.with_api_key_message(
            ApiKey::Header("X-API-KEY"),
            key,
            "USPTO requires an ODP API key",
        )?;
        Ok(self)
    }

    pub fn with_connection(connection: Connection) -> Self {
        Self { connection }
    }

    pub async fn search(&self, parameters: &[(&str, &str)]) -> Result<ApiDocument> {
        self.connection
            .json("api/v1/datasets/products/search", parameters)
            .await?
            .validated(validation::validate_products)
    }

    /// Resolve one product's file page. Total counts and release dates remain in metadata.
    pub async fn product(&self, id: &str, offset: u64, limit: u16) -> Result<RemoteFiles> {
        validation::validate_limit(limit)?;
        let id_path = filename(id)?;
        let offset = offset.to_string();
        let limit = limit.to_string();
        self.connection
            .resolve_json_files(
                &format!("api/v1/datasets/products/{id_path}"),
                &[
                    ("includeFiles", "true"),
                    ("offset", &offset),
                    ("limit", &limit),
                ],
                |doc| parser::parse_product_files(doc.value(), id),
            )
            .await
    }
}
