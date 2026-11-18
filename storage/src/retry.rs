//! Keep HTTP server hints separate from OpenDAL's operation retry machinery.
use opendal::{
    Buffer, HttpBody, HttpTransport, HttpTransporter, OperationContext,
    layers::{DefaultRetryInterceptor, RetryEvent, RetryInterceptor},
    raw::{Layer, Servicer},
};
use std::time::{Duration, SystemTime};

#[derive(Debug)]
pub(crate) struct HttpRetryAfterLayer;
impl Layer for HttpRetryAfterLayer {
    fn apply_context(&self, _: Servicer, context: OperationContext) -> OperationContext {
        let transport = RetryAfterTransport(context.http_transport().clone());
        context.with_http_transport(HttpTransporter::new(transport))
    }
}

pub(crate) struct RetryAfter {
    pub(crate) max_delay: Duration,
}
impl RetryInterceptor for RetryAfter {
    fn intercept(&self, event: RetryEvent<'_>) {
        DefaultRetryInterceptor.intercept(event);
    }

    fn retry_delay(&self, event: RetryEvent<'_>) -> Option<Duration> {
        let mut cause: &(dyn std::error::Error + 'static) = event.err;
        loop {
            if let Some(ServerDelay(server)) = cause.downcast_ref::<ServerDelay>() {
                return (*server <= self.max_delay).then(|| event.retry_after.max(*server));
            }
            match cause.source() {
                Some(source) => cause = source,
                None => return Some(event.retry_after),
            }
        }
    }
}

#[derive(Debug)]
struct ServerDelay(Duration);
impl std::fmt::Display for ServerDelay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "server requires a {:?} retry delay", self.0)
    }
}
impl std::error::Error for ServerDelay {}

struct RetryAfterTransport(HttpTransporter);
impl HttpTransport for RetryAfterTransport {
    async fn fetch(
        &self,
        request: http::Request<Buffer>,
    ) -> opendal::Result<http::Response<HttpBody>> {
        let response = self.0.fetch(request).await?;
        if !matches!(response.status().as_u16(), 429 | 503) {
            return Ok(response);
        }
        let mut error = opendal::Error::new(
            opendal::ErrorKind::Unexpected,
            "storage HTTP request was throttled",
        )
        .with_context("status", response.status())
        .set_temporary();
        if let Some(header) = response
            .headers()
            .get(http::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
        {
            error = error.with_context("retry_after", header);
            if let Some(delay) = server_delay(header) {
                error = error.set_source(ServerDelay(delay));
            }
        }
        // Drop failed bodies and their HTTP permits before the retry layer sleeps.
        drop(response);
        Err(error)
    }
}

fn server_delay(header: &str) -> Option<Duration> {
    let header = header.trim();
    if !header.is_empty() && header.bytes().all(|character| character.is_ascii_digit()) {
        return Some(Duration::from_secs(header.parse().unwrap_or(u64::MAX)));
    }
    httpdate::parse_http_date(header)
        .ok()
        .map(|date| date.duration_since(SystemTime::now()).unwrap_or_default())
}
