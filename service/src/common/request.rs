use super::{Connection, error};
use http::Request;
use open_arrow_access_core::Result;
use opendal::Buffer;
use url::Url;

impl Connection {
    pub(super) fn request(
        &self,
        url: &mut Url,
        accept: &str,
        authorized: bool,
    ) -> Result<Request<Buffer>> {
        if authorized {
            for (name, value) in &self.query {
                if !url.query_pairs().any(|(key, _)| key == name.as_str()) {
                    url.query_pairs_mut().append_pair(name, value);
                }
            }
        }
        let mut request = Request::get(url.as_str())
            .header("accept", accept)
            .body(Buffer::new())
            .map_err(error::request)?;
        if authorized {
            request.headers_mut().extend(self.headers.clone());
        }
        Ok(request)
    }
}
