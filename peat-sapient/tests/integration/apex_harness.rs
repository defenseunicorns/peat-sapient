//! Harness that starts and stops the Dstl Apex SAPIENT middleware subprocess.
//!
//! Apex acts as the HLDMM (manager) in these integration tests. Our bridge connects
//! as a DLMM (sensor-side relay) using the standard SAPIENT TCP framing.
//!
//! # CLI assumption
//!
//! The harness invokes `apex.py --port PORT`. Adjust `APEX_CMD` / `APEX_PORT_ARG` if
//! your Apex installation uses a different flag or wrapper script.
//! Repository: https://github.com/dstl/Apex-SAPIENT-Middleware

use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::TcpStream;

const APEX_CMD: &str = "apex.py";
const APEX_PORT_ARG: &str = "--port";

/// Returns `true` if `apex.py --version` succeeds (i.e. Apex is on PATH and importable).
pub fn apex_available() -> bool {
    std::process::Command::new(APEX_CMD)
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Macro-style early-return used by every integration test.
///
/// ```ignore
/// skip_if_no_apex!();
/// ```
macro_rules! skip_if_no_apex {
    () => {
        if !$crate::apex_harness::apex_available() {
            eprintln!(
                "SKIP: apex.py not on PATH — install the Dstl Apex SAPIENT middleware \
                 (https://github.com/dstl/Apex-SAPIENT-Middleware) to run integration tests."
            );
            return;
        }
    };
}
pub(crate) use skip_if_no_apex;

/// A running Apex subprocess. Killed on drop.
pub struct ApexHarness {
    pub addr: SocketAddr,
    child: tokio::process::Child,
}

impl ApexHarness {
    /// Spawn Apex on a free port and wait until it is accepting TCP connections.
    ///
    /// Panics if Apex does not become ready within 5 seconds.
    pub async fn start() -> Self {
        let port = free_port();
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

        let child = tokio::process::Command::new(APEX_CMD)
            .arg(APEX_PORT_ARG)
            .arg(port.to_string())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("failed to spawn apex.py — is it on PATH?");

        wait_for_port(addr).await;
        ApexHarness { addr, child }
    }
}

impl Drop for ApexHarness {
    fn drop(&mut self) {
        // Best-effort SIGKILL — ignore errors (process may have already exited).
        let _ = self.child.start_kill();
    }
}

/// Bind a random OS-assigned port and return its number.
///
/// There is an inherent TOCTOU window between releasing the bind and Apex claiming
/// the port; for test purposes this is acceptable.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Retry connecting to `addr` every 100 ms for up to 5 seconds.
async fn wait_for_port(addr: SocketAddr) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if TcpStream::connect(addr).await.is_ok() {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("Apex did not start within 5 s on {addr}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
