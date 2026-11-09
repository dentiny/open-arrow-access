use super::error::{decode, io};
use super::{
    ApiDocument, Connection, download_url,
    validation::{self, BodyLength},
};
use open_arrow_access_core::{Bytes, Result};
use opendal::{HttpBody, raw::oio::ReadStream};
use url::Url;

impl Connection {
    pub(super) async fn response(
        &self,
        url: Url,
        accept: &str,
    ) -> Result<http::Response<HttpBody>> {
        let mut url = download_url(url.as_str())?;
        let mut authorized = url.origin() == self.origin.origin();
        for _ in 0..=10 {
            let request = self.request(&mut url, accept, authorized)?;
            let response = self
                .context
                .http_transport()
                .fetch(request)
                .await
                .map_err(io)?;
            let status = response.status();
            if matches!(status.as_u16(), 301 | 302 | 303 | 307 | 308) {
                let next = validation::redirect(&url, response.headers())?;
                authorized = authorized && next.origin() == url.origin();
                url = next;
                continue;
            }
            if status != http::StatusCode::OK {
                // TODO: Add a shared error translation function in core and use it here.
                let kind = match status.as_u16() {
                    404 => opendal::ErrorKind::NotFound,
                    401 | 403 => opendal::ErrorKind::PermissionDenied,
                    429 => opendal::ErrorKind::RateLimited,
                    _ => opendal::ErrorKind::Unexpected,
                };
                return Err(super::error::http(kind, status));
            }
            return Ok(response);
        }
        Err(decode("too many redirects"))
    }
    /// Exact upstream JSON bytes plus a parsed view. One call performs one page request.
    pub async fn json(&self, path: &str, query: &[(&str, &str)]) -> Result<ApiDocument> {
        self.json_url(self.url(path, query)?).await
    }
    /// Fetch an API continuation URL only if it remains within the configured origin.
    pub async fn json_url(&self, url: Url) -> Result<ApiDocument> {
        validation::same_origin(&self.origin, &url)?;
        let bytes = self.bytes(url, "application/json").await?;
        ApiDocument::from_bytes(bytes)
    }
    /// Read small XML/catalogue metadata; large file content should use `open`.
    pub async fn bytes(&self, url: Url, accept: &str) -> Result<Bytes> {
        let response = self.response(url, accept).await?;
        let mut length = BodyLength::new(response.headers())?;
        let mut body = response.into_body();
        let mut bytes = Vec::new();
        loop {
            let buffer = body.read().await.map_err(io)?;
            if buffer.is_empty() {
                break;
            }
            length.push(buffer.len())?;
            for part in buffer {
                bytes.extend_from_slice(&part);
            }
        }
        length.finish()?;
        Ok(bytes.into())
    }
}
