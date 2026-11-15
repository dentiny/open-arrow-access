use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::Stream;
use http::{Request, Response};
use opendal::{
    Buffer, HttpBody, HttpTransport, HttpTransporter, OperationContext,
    raw::{Layer, Servicer},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// One HTTP request budget shared across cloned layers and wrapped transports.
///
/// Admission happens before fetching; capacity is returned on fetch failure,
/// cancellation, body EOF, body error or body drop. This limits HTTP requests,
/// not OpenDAL operations, workers or readers. No runtime or tasks are created.
/// Follows [storage-transfer-lib's transport-level limiter](https://github.com/dentiny/storage-transfer-lib/blob/241db839bee22fb9d0eedc41cd43b441d92035f0/src/http_limit.rs).
#[derive(Clone, Debug)]
pub struct HttpConcurrencyLimitLayer {
    semaphore: Arc<Semaphore>,
}

impl HttpConcurrencyLimitLayer {
    /// Reject zero or limits larger than the semaphore's supported capacity.
    pub fn new(max_ongoing_http_requests: usize) -> open_arrow_access_core::Result<Self> {
        crate::validation::validate_http_concurrency(max_ongoing_http_requests)?;
        Ok(Self {
            semaphore: Arc::new(Semaphore::new(max_ongoing_http_requests)),
        })
    }

    /// Wrap a provider transport using the same budget as layered operators.
    /// Put retries outside this transport so each attempt obtains its own permit.
    pub fn wrap(&self, inner: HttpTransporter) -> HttpTransporter {
        HttpTransporter::new(LimitedTransport {
            inner,
            semaphore: self.semaphore.clone(),
        })
    }
}

impl Layer for HttpConcurrencyLimitLayer {
    fn apply_context(&self, _: Servicer, context: OperationContext) -> OperationContext {
        let transport = self.wrap(context.http_transport().clone());
        context.with_http_transport(transport)
    }
}

struct LimitedTransport {
    inner: HttpTransporter,
    semaphore: Arc<Semaphore>,
}

impl HttpTransport for LimitedTransport {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("private semaphore is never closed");
        let response = self.inner.fetch(request).await?;
        Ok(response.map(|body| {
            body.map_inner(|inner| {
                Box::new(PermitStream {
                    inner: Some(inner),
                    permit: Some(permit),
                })
            })
        }))
    }
}

// Drop the body before returning its permit, including cancellation.
struct PermitStream<S> {
    inner: Option<S>,
    permit: Option<OwnedSemaphorePermit>,
}

impl<S: Stream<Item = opendal::Result<Buffer>> + Unpin> Stream for PermitStream<S> {
    type Item = opendal::Result<Buffer>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(None);
        };
        let result = Pin::new(inner).poll_next(cx);
        if matches!(result, Poll::Ready(None | Some(Err(_)))) {
            this.inner.take();
            this.permit.take();
        }
        result
    }
}
