//! TCP connection management for SAPIENT HLDMM and DLMM roles.

use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
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

/// A framed SAPIENT connection. The type parameter defaults to `TcpStream`
/// for plain TCP; pass a TLS stream type for encrypted connections.
pub type SapientFramed<T = TcpStream> = Framed<T, SapientCodec>;

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
pub async fn send<T: AsyncRead + AsyncWrite + Unpin>(
    framed: &mut SapientFramed<T>,
    msg: SapientMessage,
) -> Result<(), SapientError> {
    framed.send(msg).await
}

/// Send pre-encoded protobuf bytes over a framed connection, avoiding a
/// decode/re-encode round-trip when the payload is already serialised.
pub async fn send_raw<T: AsyncRead + AsyncWrite + Unpin>(
    framed: &mut SapientFramed<T>,
    bytes: Vec<u8>,
) -> Result<(), SapientError> {
    framed.send(bytes).await
}

/// Receive a single `SapientMessage` from a framed connection.
/// Returns `None` if the peer closed the connection.
pub async fn recv<T: AsyncRead + AsyncWrite + Unpin>(
    framed: &mut SapientFramed<T>,
) -> Result<Option<SapientMessage>, SapientError> {
    match framed.next().await {
        Some(Ok(msg)) => Ok(Some(msg)),
        Some(Err(e)) => Err(e),
        None => {
            debug!("SAPIENT peer closed connection");
            Ok(None)
        }
    }
}

// ── TLS support ──────────────────────────────────────────────────────────

#[cfg(feature = "tls")]
pub use tls::*;

#[cfg(feature = "tls")]
mod tls {
    use super::*;
    use std::path::Path;
    use std::sync::Arc;
    use tokio_rustls::rustls;

    /// TLS configuration for a SAPIENT connection.
    #[derive(Debug, Clone)]
    pub struct SapientTlsConfig {
        inner: Arc<SapientTlsConfigInner>,
    }

    #[derive(Debug)]
    struct SapientTlsConfigInner {
        client_config: Option<rustls::ClientConfig>,
        server_config: Option<rustls::ServerConfig>,
    }

    impl SapientTlsConfig {
        /// Build a TLS client config (DLMM connecting to HLDMM).
        ///
        /// - `ca_cert`: CA certificate PEM for server verification.
        /// - `client_cert` + `client_key`: optional mTLS identity.
        pub fn client(
            ca_cert: &Path,
            client_cert: Option<&Path>,
            client_key: Option<&Path>,
        ) -> Result<Self, SapientError> {
            let ca_file = std::fs::File::open(ca_cert)
                .map_err(|e| SapientError::ConnectionFailed(format!("open CA cert: {e}")))?;
            let ca_certs: Vec<_> = rustls_pemfile::certs(&mut std::io::BufReader::new(ca_file))
                .collect::<Result<_, _>>()
                .map_err(|e| SapientError::ConnectionFailed(format!("parse CA cert PEM: {e}")))?;

            let mut root_store = rustls::RootCertStore::empty();
            for cert in ca_certs {
                root_store
                    .add(cert)
                    .map_err(|e| SapientError::ConnectionFailed(format!("add CA cert: {e}")))?;
            }

            // FIPS-approved cipher suites only.
            let provider = fips_crypto_provider();

            let config = if let (Some(cert_path), Some(key_path)) = (client_cert, client_key) {
                let certs = load_certs(cert_path)?;
                let key = load_key(key_path)?;
                rustls::ClientConfig::builder_with_provider(provider.into())
                    .with_safe_default_protocol_versions()
                    .map_err(|e| {
                        SapientError::ConnectionFailed(format!("TLS version config: {e}"))
                    })?
                    .with_root_certificates(root_store)
                    .with_client_auth_cert(certs, key)
                    .map_err(|e| SapientError::ConnectionFailed(format!("TLS client auth: {e}")))?
            } else {
                rustls::ClientConfig::builder_with_provider(provider.into())
                    .with_safe_default_protocol_versions()
                    .map_err(|e| {
                        SapientError::ConnectionFailed(format!("TLS version config: {e}"))
                    })?
                    .with_root_certificates(root_store)
                    .with_no_client_auth()
            };

            Ok(Self {
                inner: Arc::new(SapientTlsConfigInner {
                    client_config: Some(config),
                    server_config: None,
                }),
            })
        }

        /// Build a TLS server config (HLDMM accepting DLMMs).
        ///
        /// - `server_cert` + `server_key`: server identity.
        /// - `ca_cert`: optional CA cert for client verification (mTLS).
        pub fn server(
            server_cert: &Path,
            server_key: &Path,
            ca_cert: Option<&Path>,
        ) -> Result<Self, SapientError> {
            let certs = load_certs(server_cert)?;
            let key = load_key(server_key)?;
            let provider = fips_crypto_provider();

            let config = if let Some(ca_path) = ca_cert {
                let ca_file = std::fs::File::open(ca_path)
                    .map_err(|e| SapientError::ConnectionFailed(format!("open CA cert: {e}")))?;
                let ca_certs: Vec<_> = rustls_pemfile::certs(&mut std::io::BufReader::new(ca_file))
                    .collect::<Result<_, _>>()
                    .map_err(|e| SapientError::ConnectionFailed(format!("parse CA cert: {e}")))?;
                let mut root_store = rustls::RootCertStore::empty();
                for cert in ca_certs {
                    root_store
                        .add(cert)
                        .map_err(|e| SapientError::ConnectionFailed(format!("add CA cert: {e}")))?;
                }
                let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
                    .build()
                    .map_err(|e| {
                        SapientError::ConnectionFailed(format!("client verifier build: {e}"))
                    })?;
                rustls::ServerConfig::builder_with_provider(provider.into())
                    .with_safe_default_protocol_versions()
                    .map_err(|e| {
                        SapientError::ConnectionFailed(format!("TLS version config: {e}"))
                    })?
                    .with_client_cert_verifier(verifier)
                    .with_single_cert(certs, key)
                    .map_err(|e| SapientError::ConnectionFailed(format!("TLS server cert: {e}")))?
            } else {
                rustls::ServerConfig::builder_with_provider(provider.into())
                    .with_safe_default_protocol_versions()
                    .map_err(|e| {
                        SapientError::ConnectionFailed(format!("TLS version config: {e}"))
                    })?
                    .with_no_client_auth()
                    .with_single_cert(certs, key)
                    .map_err(|e| SapientError::ConnectionFailed(format!("TLS server cert: {e}")))?
            };

            Ok(Self {
                inner: Arc::new(SapientTlsConfigInner {
                    client_config: None,
                    server_config: Some(config),
                }),
            })
        }

        pub(crate) fn client_connector(&self) -> Option<tokio_rustls::TlsConnector> {
            self.inner
                .client_config
                .as_ref()
                .map(|c| tokio_rustls::TlsConnector::from(Arc::new(c.clone())))
        }

        pub(crate) fn server_acceptor(&self) -> Option<tokio_rustls::TlsAcceptor> {
            self.inner
                .server_config
                .as_ref()
                .map(|c| tokio_rustls::TlsAcceptor::from(Arc::new(c.clone())))
        }
    }

    /// Connect to a SAPIENT peer over TLS.
    pub async fn connect_tls(
        addr: SocketAddr,
        tls: &SapientTlsConfig,
        server_name: &str,
    ) -> Result<SapientFramed<tokio_rustls::client::TlsStream<TcpStream>>, SapientError> {
        let connector = tls.client_connector().ok_or_else(|| {
            SapientError::ConnectionFailed("TLS config has no client config".into())
        })?;
        let name = rustls::pki_types::ServerName::try_from(server_name.to_string())
            .map_err(|e| SapientError::ConnectionFailed(format!("invalid server name: {e}")))?;
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| SapientError::ConnectionFailed(e.to_string()))?;
        let tls_stream = connector
            .connect(name, stream)
            .await
            .map_err(|e| SapientError::ConnectionFailed(format!("TLS handshake: {e}")))?;
        info!(%addr, "SAPIENT TLS connection established");
        Ok(Framed::new(tls_stream, SapientCodec))
    }

    /// Connect to a SAPIENT peer over TLS, retrying with exponential backoff.
    pub async fn connect_tls_with_retry(
        addr: SocketAddr,
        tls: &SapientTlsConfig,
        server_name: &str,
        config: &ReconnectConfig,
    ) -> Result<SapientFramed<tokio_rustls::client::TlsStream<TcpStream>>, SapientError> {
        let mut delay = config.initial_delay;
        loop {
            match connect_tls(addr, tls, server_name).await {
                Ok(framed) => return Ok(framed),
                Err(e) => {
                    warn!(%addr, error = %e, retry_in = ?delay, "SAPIENT TLS connection failed, retrying");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(config.max_delay);
                }
            }
        }
    }

    /// Accept a single inbound SAPIENT TLS connection.
    pub async fn accept_tls(
        listener: &TcpListener,
        tls: &SapientTlsConfig,
    ) -> Result<
        (
            SapientFramed<tokio_rustls::server::TlsStream<TcpStream>>,
            SocketAddr,
        ),
        SapientError,
    > {
        let acceptor = tls.server_acceptor().ok_or_else(|| {
            SapientError::ConnectionFailed("TLS config has no server config".into())
        })?;
        let (stream, peer_addr) = listener
            .accept()
            .await
            .map_err(|e| SapientError::ConnectionFailed(e.to_string()))?;
        let tls_stream = acceptor
            .accept(stream)
            .await
            .map_err(|e| SapientError::ConnectionFailed(format!("TLS accept: {e}")))?;
        debug!(%peer_addr, "accepted SAPIENT TLS connection");
        Ok((Framed::new(tls_stream, SapientCodec), peer_addr))
    }

    fn fips_crypto_provider() -> rustls::crypto::CryptoProvider {
        rustls::crypto::CryptoProvider {
            cipher_suites: vec![
                rustls::crypto::aws_lc_rs::cipher_suite::TLS13_AES_256_GCM_SHA384,
                rustls::crypto::aws_lc_rs::cipher_suite::TLS13_AES_128_GCM_SHA256,
                rustls::crypto::aws_lc_rs::cipher_suite::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
                rustls::crypto::aws_lc_rs::cipher_suite::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
                rustls::crypto::aws_lc_rs::cipher_suite::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
                rustls::crypto::aws_lc_rs::cipher_suite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
            ],
            ..rustls::crypto::aws_lc_rs::default_provider()
        }
    }

    fn load_certs(
        path: &Path,
    ) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, SapientError> {
        let file = std::fs::File::open(path)
            .map_err(|e| SapientError::ConnectionFailed(format!("open cert {path:?}: {e}")))?;
        rustls_pemfile::certs(&mut std::io::BufReader::new(file))
            .collect::<Result<_, _>>()
            .map_err(|e| SapientError::ConnectionFailed(format!("parse cert PEM: {e}")))
    }

    fn load_key(path: &Path) -> Result<rustls::pki_types::PrivateKeyDer<'static>, SapientError> {
        let file = std::fs::File::open(path)
            .map_err(|e| SapientError::ConnectionFailed(format!("open key {path:?}: {e}")))?;
        rustls_pemfile::private_key(&mut std::io::BufReader::new(file))
            .map_err(|e| SapientError::ConnectionFailed(format!("parse key PEM: {e}")))?
            .ok_or_else(|| {
                SapientError::ConnectionFailed("no private key found in PEM file".into())
            })
    }
}
