use super::error::{OP, decode, invalid, io};
use super::{ApiDocument, Connection, download_url};
use http::Request;
use open_arrow_access_core::{Bytes, Result};
use opendal::{Buffer, HttpBody, raw::oio::ReadStream};
use url::Url;

impl Connection {
    pub(super) async fn response(
        &self,
        url: Url,
        accept: &str,
    ) -> Result<http::Response<HttpBody>> {
        let mut url = download_url(url.as_str())?;
        let mut authorized = url.origin() == self.origin.origin();
        if authorized && !self.query.is_empty() {
            url.query_pairs_mut()
                .extend_pairs(self.query.iter().map(|(k, v)| (k, v)));
        }
        for _ in 0..=10 {
            let mut request = Request::get(url.as_str())
                .header("accept", accept)
                .body(Buffer::new())
                .map_err(|e| invalid("invalid HTTP request").with_source(e))?;
            if authorized {
                request.headers_mut().extend(self.headers.clone());
            }
            let response = self
                .context
                .http_transport()
                .fetch(request)
                .await
                .map_err(io)?;
            let status = response.status();
            if matches!(status.as_u16(), 301 | 302 | 303 | 307 | 308) {
                let location = response
                    .headers()
                    .get("location")
                    .and_then(|v| v.to_str().ok())
                    .ok_or_else(|| decode("redirect lacks Location"))?;
                let next = download_url(
                    url.join(location)
                        .map_err(|_| decode("invalid redirect"))?
                        .as_str(),
                )?;
                if url.scheme() == "https" && next.scheme() != "https" {
                    return Err(decode("HTTPS downgrade redirect"));
                }
                authorized = authorized && next.origin() == url.origin();
                url = next;
                continue;
            }
            crate::http_client::check_status(status, response.headers(), OP)?;
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
        if url.origin() != self.origin.origin() {
            return Err(invalid("API continuation changed origin"));
        }
        let bytes = self.bytes(url, "application/json").await?;
        let value = serde_json::from_slice(&bytes)
            .map_err(|e| decode("invalid JSON response").with_source(e))?;
        Ok(ApiDocument { bytes, value })
    }
    /// Read small XML/catalogue metadata; large file content should use `open`.
    pub async fn bytes(&self, url: Url, accept: &str) -> Result<Bytes> {
        let mut body = self.response(url, accept).await?.into_body();
        let mut bytes = Vec::new();
        loop {
            let buffer = body.read().await.map_err(io)?;
            if buffer.is_empty() {
                break;
            }
            for part in buffer {
                bytes.extend_from_slice(&part);
            }
        }
        Ok(bytes.into())
    }
}
