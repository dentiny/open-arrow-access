//! Shared single-request admission for HTTP transports and decoded API responses.
use std::{sync::Arc, time::Duration};

use http::{Request, Response};
use opendal::{
    Buffer, HttpBody, HttpTransport, HttpTransporter, OperationContext,
    raw::{Layer, Servicer},
};
use tokio::{
    sync::{Mutex, OwnedMutexGuard},
    time::{Instant, sleep_until},
};

/// Serialize HTTP requests and enforce a minimum interval between their starts.
/// Clones share admission; independent instances have independent budgets.
/// Share one instance per origin: URLs are not inspected or partitioned.
///
/// Unlike [`crate::HttpReadRateLimitLayer`], this gates each HTTP transport call
/// and keeps only one response body active. It does not classify throttle
/// responses or implement Retry-After. Put retries outside the transport so
/// every attempt obtains admission.
///
/// ```no_run
/// use std::time::Duration;
/// use open_arrow_access_storage::{HttpRequestRateLimitLayer, opendal};
///
/// fn paced(operator: opendal::Operator, limit: HttpRequestRateLimitLayer)
///     -> opendal::Operator
/// {
///     operator.layer(limit)
/// }
/// # fn main() -> open_arrow_access_core::Result<()> {
/// let limit = HttpRequestRateLimitLayer::new(Duration::from_secs(3))?;
/// // Share limit.clone() across operators, or use limit.wrap(transport).
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct HttpRequestRateLimitLayer {
    interval: Duration,
    next: Arc<Mutex<Option<Instant>>>,
}

impl HttpRequestRateLimitLayer {
    /// Reject zero or an interval that cannot be represented by the clock.
    pub fn new(min_request_interval: Duration) -> open_arrow_access_core::Result<Self> {
        crate::validation::validate_http_request_interval(min_request_interval)?;
        Ok(Self {
            interval: min_request_interval,
            next: Arc::new(Mutex::new(None)),
        })
    }

    /// Wait for both the previous permit to be released and the start interval.
    /// Keep the permit through application decoding when that work must also
    /// be serialized. Cancellation releases the lock without reserving a start.
    /// Do not acquire manually around a transport wrapped by the same limiter.
    pub async fn acquire(&self) -> HttpRequestRateLimitPermit {
        let mut next = self.next.clone().lock_owned().await;
        if let Some(deadline) = *next {
            sleep_until(deadline).await;
        }
        *next = Some(Instant::now() + self.interval);
        HttpRequestRateLimitPermit { _guard: next }
    }

    /// Hold admission until fetch failure/cancellation or body EOF/error/drop.
    /// A response left unread retains its permit until dropped.
    pub fn wrap(&self, inner: HttpTransporter) -> HttpTransporter {
        HttpTransporter::new(LimitedTransport {
            inner,
            limit: self.clone(),
        })
    }
}

impl Layer for HttpRequestRateLimitLayer {
    fn apply_context(&self, _: Servicer, context: OperationContext) -> OperationContext {
        let transport = self.wrap(context.http_transport().clone());
        context.with_http_transport(transport)
    }
}

/// Admission held until dropped, including when the owning task is cancelled.
#[derive(Debug)]
pub struct HttpRequestRateLimitPermit {
    _guard: OwnedMutexGuard<Option<Instant>>,
}

struct LimitedTransport {
    inner: HttpTransporter,
    limit: HttpRequestRateLimitLayer,
}

impl HttpTransport for LimitedTransport {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let permit = self.limit.acquire().await;
        let response = self.inner.fetch(request).await?;
        Ok(response.map(|body| crate::http_limit::hold_permit(body, permit)))
    }
}
