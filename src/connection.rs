//! TCP connection management for SAPIENT HLDMM and DLMM roles.

use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use tracing::{debug, info, warn};

use crate::{codec::SapientCodec, error::SapientError, proto::SapientMessage};

/// How this bridge participates in the SAPIENT topology.
#[derive(Debug, Clone)]
pub enum BridgeRole {
    /// Act as the HLDMM (manager): listen for incoming DLMM connections.
    Hldmm { listen_addr: SocketAddr },
    /// Act as a DLMM (sensor-side relay): connect to an ASM or HLDMM.
    Dlmm {
        remote_addr: SocketAddr,
        reconnect: ReconnectConfig,
    },
}

/// Reconnect policy for DLMM mode.
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// Initial backoff before first retry.
    pub initial_delay: Duration,
    /// Maximum backoff between retries.
    pub max_delay: Duration,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
        }
    }
}

/// A framed SAPIENT TCP connection.
pub type SapientFramed = Framed<TcpStream, SapientCodec>;

/// Establish a single outbound TCP connection to a SAPIENT peer.
/// Returns the framed stream on success.
pub async fn connect(addr: SocketAddr) -> Result<SapientFramed, SapientError> {
    let stream = TcpStream::connect(addr)
        .await
        .map_err(|e| SapientError::ConnectionFailed(e.to_string()))?;
    info!(%addr, "SAPIENT connection established");
    Ok(Framed::new(stream, SapientCodec))
}

/// Connect to a SAPIENT peer, retrying with exponential backoff on failure.
pub async fn connect_with_retry(
    addr: SocketAddr,
    config: &ReconnectConfig,
) -> Result<SapientFramed, SapientError> {
    let mut delay = config.initial_delay;
    loop {
        match connect(addr).await {
            Ok(framed) => return Ok(framed),
            Err(e) => {
                warn!(%addr, error = %e, retry_in = ?delay, "SAPIENT connection failed, retrying");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(config.max_delay);
            }
        }
    }
}

/// Accept a single inbound SAPIENT connection from a TCP listener.
pub async fn accept(listener: &TcpListener) -> Result<(SapientFramed, SocketAddr), SapientError> {
    let (stream, peer_addr) = listener
        .accept()
        .await
        .map_err(|e| SapientError::ConnectionFailed(e.to_string()))?;
    debug!(%peer_addr, "accepted SAPIENT connection");
    Ok((Framed::new(stream, SapientCodec), peer_addr))
}

/// Send a single `SapientMessage` over a framed connection.
pub async fn send(
    framed: &mut SapientFramed,
    msg: SapientMessage,
) -> Result<(), SapientError> {
    framed.send(msg).await
}

/// Receive a single `SapientMessage` from a framed connection.
/// Returns `None` if the peer closed the connection.
pub async fn recv(framed: &mut SapientFramed) -> Result<Option<SapientMessage>, SapientError> {
    match framed.next().await {
        Some(Ok(msg)) => Ok(Some(msg)),
        Some(Err(e)) => Err(e),
        None => {
            debug!("SAPIENT peer closed connection");
            Ok(None)
        }
    }
}
