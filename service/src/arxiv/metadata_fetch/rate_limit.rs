//! Coordinate automatic OAI lookups across services using the official origin.
use crate::common::Connection;
use open_arrow_access_storage::HttpRequestRateLimitLayer;
use std::sync::LazyLock;
use std::time::Duration;

// All official metadata connections share one request budget.
static OFFICIAL_REQUESTS: LazyLock<HttpRequestRateLimitLayer> = LazyLock::new(|| {
    HttpRequestRateLimitLayer::new(REQUEST_INTERVAL).expect("valid official request interval")
});
// arXiv's legacy API policy includes OAI-PMH and permits at most one request
// every three seconds: https://info.arxiv.org/help/api/tou.html#rate-limits
const REQUEST_INTERVAL: Duration = Duration::from_secs(3);

pub(super) fn apply(connection: Connection) -> Connection {
    let official = connection
        .origin()
        .host_str()
        .is_some_and(|host| host == "arxiv.org" || host.ends_with(".arxiv.org"));
    if official {
        connection.with_layer(OFFICIAL_REQUESTS.clone())
    } else {
        // Caller-selected mirrors have their own rate policies.
        connection
    }
}
