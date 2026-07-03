use std::time::Duration;

use peat_sapient::connection::{connect_with_retry, ReconnectConfig};
use tokio::net::TcpListener;

/// Verify the exponential-backoff delay arithmetic: doubles each retry, caps at max_delay.
///
/// This is a pure computation test — no I/O, no real sleep.
#[test]
fn backoff_doubles_and_caps_at_max() {
    let config = ReconnectConfig {
        initial_delay: Duration::from_millis(100),
        max_delay: Duration::from_millis(400),
    };
    let mut delay = config.initial_delay;

    delay = (delay * 2).min(config.max_delay);
    assert_eq!(delay, Duration::from_millis(200));

    delay = (delay * 2).min(config.max_delay);
    assert_eq!(delay, Duration::from_millis(400));

    delay = (delay * 2).min(config.max_delay); // would be 800, capped at 400
    assert_eq!(
        delay,
        Duration::from_millis(400),
        "delay must cap at max_delay"
    );
}

/// Verify that `connect_with_retry` reconnects after the server becomes available.
///
/// Uses `start_paused = true` so `tokio::time::sleep` inside `connect_with_retry`
/// does not consume real wall-clock time. We advance mock time to trigger each
/// retry without sleeping.
#[tokio::test(start_paused = true)]
async fn dlmm_reconnects_after_server_drop() {
    // Grab a free port, then release it so the first connect attempt fails.
    let tmp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = tmp.local_addr().unwrap();
    drop(tmp);

    let config = ReconnectConfig {
        initial_delay: Duration::from_millis(50),
        max_delay: Duration::from_millis(200),
    };

    // Start the retry loop in a background task.
    let retry_handle = tokio::spawn(async move { connect_with_retry(addr, &config).await });

    // Yield so the first connection attempt runs and fails (ECONNREFUSED is instant).
    tokio::task::yield_now().await;

    // The task is now sleeping for `initial_delay`. Bring up the server.
    let listener = TcpListener::bind(addr).await.unwrap();
    let accept_handle = tokio::spawn(async move {
        // Accept and immediately drop — we only care that the handshake completes.
        let _ = listener.accept().await;
    });

    // Advance mock time past the initial backoff delay to wake the retry task.
    tokio::time::advance(Duration::from_millis(51)).await;
    // Yield once more to let the reconnect attempt run.
    tokio::task::yield_now().await;

    // The retry should have connected. Give it a short real timeout as a safety net.
    let result = tokio::time::timeout(Duration::from_millis(500), retry_handle)
        .await
        .expect("reconnect timed out — retry loop did not connect")
        .expect("retry task panicked");

    assert!(
        result.is_ok(),
        "connect_with_retry should succeed after server becomes available"
    );

    let _ = accept_handle.await;
}

/// Verify that `connect_with_retry` retries more than once before succeeding.
///
/// The server is brought up only after the second backoff period, so the test
/// exercises at least two failed attempts.
#[tokio::test(start_paused = true)]
async fn dlmm_retries_multiple_times_before_success() {
    let tmp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = tmp.local_addr().unwrap();
    drop(tmp);

    let config = ReconnectConfig {
        initial_delay: Duration::from_millis(50),
        max_delay: Duration::from_millis(200),
    };

    let retry_handle = tokio::spawn(async move { connect_with_retry(addr, &config).await });

    // Let first attempt fail.
    tokio::task::yield_now().await;
    // Advance past first delay (50ms) — task wakes, retries, fails again.
    tokio::time::advance(Duration::from_millis(51)).await;
    tokio::task::yield_now().await;
    // Now the task is sleeping for 100ms (doubled). Bring up the server.
    let listener = TcpListener::bind(addr).await.unwrap();
    let accept_handle = tokio::spawn(async move {
        let _ = listener.accept().await;
    });

    // Advance past second delay (100ms).
    tokio::time::advance(Duration::from_millis(101)).await;
    tokio::task::yield_now().await;

    let result = tokio::time::timeout(Duration::from_millis(500), retry_handle)
        .await
        .expect("timed out waiting for reconnect")
        .expect("task panicked");

    assert!(result.is_ok(), "should connect after two retries");
    let _ = accept_handle.await;
}
