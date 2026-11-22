//! Coordinate automatic OAI lookups across services using the official origin.
use crate::common::Connection;
use futures::lock::{Mutex, MutexGuard};
use std::time::Duration;
use tokio::time::{Instant, sleep_until};

// https://info.arxiv.org/help/api/tou.html: one request every three seconds,
// with one connection at a time. Keep the guard until the body is decoded.
static OFFICIAL_REQUESTS: Mutex<Option<Instant>> = Mutex::new(None);
const REQUEST_INTERVAL: Duration = Duration::from_secs(3);

pub(super) async fn acquire(
    connection: &Connection,
) -> Option<MutexGuard<'static, Option<Instant>>> {
    let host = connection.origin().host_str()?;
    if host == "arxiv.org" || host.ends_with(".arxiv.org") {
        Some(reserve(&OFFICIAL_REQUESTS).await)
    } else {
        // Caller-selected mirrors have their own rate policies.
        None
    }
}

async fn reserve(gate: &Mutex<Option<Instant>>) -> MutexGuard<'_, Option<Instant>> {
    let mut next = gate.lock().await;
    if let Some(deadline) = *next {
        sleep_until(deadline).await;
    }
    *next = Some(Instant::now() + REQUEST_INTERVAL);
    next
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{FutureExt, pin_mut, poll};
    use std::task::Poll;

    #[tokio::test(start_paused = true)]
    async fn requests_wait_for_both_spacing_and_previous_body() {
        let gate = Mutex::new(None);
        let start = Instant::now();
        let first = reserve(&gate).await;
        let second = reserve(&gate).fuse();
        pin_mut!(second);
        assert!(matches!(poll!(&mut second), Poll::Pending));
        tokio::time::advance(Duration::from_secs(4)).await;
        assert!(matches!(poll!(&mut second), Poll::Pending));
        drop(first);
        let second = second.await;
        assert_eq!(Instant::now() - start, Duration::from_secs(4));
        drop(second);
        let _third = reserve(&gate).await;
        assert_eq!(Instant::now() - start, Duration::from_secs(7));
    }
}
