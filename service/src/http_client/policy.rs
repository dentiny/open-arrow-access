//! Provider HTTP policy on OpenDAL's transport plane. File operators retain
//! their native StorageConfig layers; this adapter covers direct API requests.
use backon::{ExponentialBuilder, Retryable};
use futures::{StreamExt, stream};
use http::{Request, Response};
use open_arrow_access_core::Result;
use open_arrow_access_storage::StorageConfig;
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter, OperationContext};
use tokio::time::timeout;

use super::{
    error::{Failure, classify_response, finish_response, timeout_error},
    validation::is_retry_safe_method,
};

pub(crate) fn apply(config: &StorageConfig, context: OperationContext) -> Result<OperationContext> {
    config.validate()?;
    if config.retry.is_none() && config.timeout.is_none() {
        return Ok(context);
    }
    let inner = context.http_transport().clone();
    Ok(
        context.with_http_transport(HttpTransporter::new(PolicyTransport {
            inner,
            config: config.clone(),
        })),
    )
}

#[derive(Debug)]
struct PolicyTransport {
    inner: HttpTransporter,
    config: StorageConfig,
}

impl HttpTransport for PolicyTransport {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let safe = is_retry_safe_method(request.method());
        let fetch = || async {
            let result = match &self.config.timeout {
                Some(policy) => {
                    timeout(policy.operation_timeout, self.inner.fetch(request.clone()))
                        .await
                        .unwrap_or_else(|_| Err(timeout_error()))
                }
                None => self.inner.fetch(request.clone()).await,
            };
            classify_response(result)
        };
        let result = if let Some(policy) = self.config.retry.as_ref().filter(|_| safe) {
            let mut backoff = ExponentialBuilder::default()
                .with_max_times(policy.max_retries)
                .with_min_delay(policy.min_delay)
                .with_max_delay(policy.max_delay)
                .with_factor(policy.factor);
            if policy.jitter {
                backoff = backoff.with_jitter();
            }
            fetch
                .retry(backoff)
                .when(Failure::is_retryable)
                .adjust(|failure, delay| {
                    // Never revive an exhausted retry budget. A server delay
                    // beyond our bound terminates rather than retrying early.
                    let delay = delay?;
                    let server = failure.retry_after();
                    match server {
                        Some(server) if server > policy.max_delay => None,
                        Some(server) => Some(delay.max(server)),
                        None => Some(delay),
                    }
                })
                .await
        } else {
            fetch().await
        };
        let response = finish_response(result)?;
        let Some(policy) = &self.config.timeout else {
            return Ok(response);
        };
        let duration = policy.io_timeout;
        // Each body poll has its own deadline. Never replay a successful
        // response after handing any bytes to the consumer.
        Ok(response.map(|body| {
            body.map_inner(|inner| {
                Box::new(Box::pin(stream::try_unfold(
                    inner,
                    move |mut inner| async move {
                        match timeout(duration, inner.next()).await {
                            Ok(Some(buffer)) => Ok(Some((buffer?, inner))),
                            Ok(None) => Ok(None),
                            Err(_) => Err(timeout_error()),
                        }
                    },
                )))
            })
        }))
    }
}
