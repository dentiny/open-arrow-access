//! Coordinate automatic OAI lookups across services using the official origin.
use crate::common::Connection;
use open_arrow_access_storage::{HttpRequestRateLimitLayer, HttpRequestRateLimitPermit};
use std::sync::LazyLock;
use std::time::Duration;

// Keep the guard until the body is decoded to allow only one lookup at a time.
static OFFICIAL_REQUESTS: LazyLock<HttpRequestRateLimitLayer> = LazyLock::new(|| {
    HttpRequestRateLimitLayer::new(REQUEST_INTERVAL).expect("valid official request interval")
});
// arXiv's legacy API policy includes OAI-PMH and permits at most one request
// every three seconds: https://info.arxiv.org/help/api/tou.html#rate-limits
const REQUEST_INTERVAL: Duration = Duration::from_secs(3);

pub(super) async fn acquire(connection: &Connection) -> Option<HttpRequestRateLimitPermit> {
    let host = connection.origin().host_str()?;
    if host == "arxiv.org" || host.ends_with(".arxiv.org") {
        Some(OFFICIAL_REQUESTS.acquire().await)
    } else {
        // Caller-selected mirrors have their own rate policies.
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{pin_mut, poll};
    use tokio::time::Instant;

    #[tokio::test(start_paused = true)]
    async fn official_origins_share_admission_and_mirrors_bypass_it() {
        let official = Connection::new("https://arxiv.org").unwrap();
        let subdomain = Connection::new("https://export.arxiv.org").unwrap();
        let first = acquire(&official).await.unwrap();
        for origin in ["https://mirror.invalid", "https://arxiv.org.mirror.invalid"] {
            let mirror = Connection::new(origin).unwrap();
            assert!(matches!(
                poll!(Box::pin(acquire(&mirror))),
                std::task::Poll::Ready(None)
            ));
        }
        let start = Instant::now();
        let second = acquire(&subdomain);
        pin_mut!(second);
        assert!(poll!(&mut second).is_pending());
        drop(first);
        let _second = second.await.unwrap();
        assert_eq!(Instant::now() - start, REQUEST_INTERVAL);
    }
}
