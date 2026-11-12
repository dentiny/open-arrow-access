//! Provider HTTP policy on OpenDAL's transport plane. File operators retain
//! their native StorageConfig layers; this adapter covers direct API requests.
use std::time::{Duration, SystemTime};

use backon::{ExponentialBuilder, Retryable};
use futures::{StreamExt, stream};
use http::{Request, Response};
use open_arrow_access_core::Result;
use open_arrow_access_storage::StorageConfig;
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter, OperationContext};
use tokio::time::timeout;

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

// Keep HTTP responses intact until retries end: provider status classification
// retains the final response's status and unmodified Retry-After header.
enum Failure {
    Transport(opendal::Error),
    Response(Response<HttpBody>),
}

impl HttpTransport for PolicyTransport {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let safe = matches!(*request.method(), http::Method::GET | http::Method::HEAD);
        let fetch = || async {
            let result = match &self.config.timeout {
                Some(policy) => {
                    timeout(policy.operation_timeout, self.inner.fetch(request.clone()))
                        .await
                        .unwrap_or_else(|_| Err(timeout_error()))
                }
                None => self.inner.fetch(request.clone()).await,
            };
            match result {
                Ok(response) if retry_status(response.status().as_u16()) => {
                    Err(Failure::Response(response))
                }
                Ok(response) => Ok(response),
                Err(error) => Err(Failure::Transport(error)),
            }
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
                .when(|failure| match failure {
                    Failure::Transport(error) => error.is_temporary(),
                    Failure::Response(_) => true,
                })
                .adjust(|failure, delay| {
                    // Never revive an exhausted retry budget. A server delay
                    // beyond our bound terminates rather than retrying early.
                    let delay = delay?;
                    let server = match failure {
                        Failure::Response(response) => {
                            retry_after(response.headers(), SystemTime::now())
                        }
                        Failure::Transport(_) => None,
                    };
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
        let response = match result {
            Ok(response) | Err(Failure::Response(response)) => response,
            Err(Failure::Transport(error)) => return Err(error),
        };
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

fn retry_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500 | 502 | 503 | 504)
}

fn timeout_error() -> opendal::Error {
    opendal::Error::new(opendal::ErrorKind::Unexpected, "provider HTTP timeout")
        .set_source(open_arrow_access_core::Error::new(
            open_arrow_access_core::ErrorKind::Timeout,
            "provider_http",
            "provider HTTP timeout",
        ))
        .set_temporary()
}

fn retry_after(headers: &http::HeaderMap, now: SystemTime) -> Option<Duration> {
    let value = headers
        .get(http::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
        // Overflow is an excessive server delay, not permission to retry early.
        return Some(Duration::from_secs(value.parse().unwrap_or(u64::MAX)));
    }
    Some(
        httpdate::parse_http_date(value)
            .ok()?
            .duration_since(now)
            .unwrap_or_default(),
    )
}
