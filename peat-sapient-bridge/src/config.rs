use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use serde::Deserialize;

#[derive(Parser, Debug)]
#[command(
    name = "peat-sapient-bridge",
    about = "Bidirectional SAPIENT ↔ mesh bridge"
)]
pub struct Cli {
    /// Path to TOML config file.
    #[arg(long, short)]
    pub config: Option<PathBuf>,

    /// Node name (used as mesh identity seed — same name = same NodeId).
    #[arg(long)]
    pub name: Option<String>,

    /// Mesh bind address for iroh QUIC endpoint.
    #[arg(long)]
    pub bind: Option<SocketAddr>,

    /// Static mesh peer: NODE_ID_HEX@ADDR. Repeatable.
    #[arg(long)]
    pub peer: Vec<String>,

    /// SAPIENT listen address (HLDMM mode).
    #[arg(long)]
    pub sapient_listen: Option<SocketAddr>,

    /// SAPIENT remote address (DLMM mode — connect to external HLDMM).
    #[arg(long)]
    pub sapient_remote: Option<SocketAddr>,

    /// Peer node ID when in DLMM mode.
    #[arg(long)]
    pub sapient_peer_id: Option<String>,

    /// Enable or disable TLS for the SAPIENT connection (overrides config file).
    #[arg(long, num_args = 0..=1, default_missing_value = "true", value_parser = clap::value_parser!(bool))]
    pub sapient_tls: Option<bool>,

    /// Server/client certificate PEM for SAPIENT TLS.
    #[arg(long)]
    pub sapient_cert: Option<PathBuf>,

    /// Private key PEM for SAPIENT TLS.
    #[arg(long)]
    pub sapient_key: Option<PathBuf>,

    /// CA certificate PEM for SAPIENT TLS peer verification.
    #[arg(long)]
    pub sapient_ca_cert: Option<PathBuf>,

    /// TLS server name for SAPIENT SNI (DLMM mode only).
    #[arg(long)]
    pub sapient_server_name: Option<String>,

    /// Persistence directory. Defaults to a tempdir.
    #[arg(long)]
    pub storage: Option<PathBuf>,

    /// TAK Server address (host:port). Enables TAK transport when set.
    #[arg(long)]
    pub tak_server: Option<SocketAddr>,

    /// Enable or disable TLS for the TAK Server connection (overrides config file).
    #[arg(long, num_args = 0..=1, default_missing_value = "true", value_parser = clap::value_parser!(bool))]
    pub tak_tls: Option<bool>,

    /// Client certificate PEM for TAK mTLS.
    #[arg(long)]
    pub tak_cert: Option<PathBuf>,

    /// Client private key PEM for TAK mTLS.
    #[arg(long)]
    pub tak_key: Option<PathBuf>,

    /// CA certificate PEM for TAK server verification.
    #[arg(long)]
    pub tak_ca_cert: Option<PathBuf>,

    /// Callsign for TAK identification (default: "Peat-BRIDGE").
    #[arg(long)]
    pub tak_callsign: Option<String>,

    /// TLS server name for TAK SNI (hostname from the server cert).
    #[arg(long)]
    pub tak_server_name: Option<String>,

    /// Mesh-side node ID for the TAK Server peer.
    #[arg(long, default_value = "tak-server-0")]
    pub tak_peer_id: String,
}

#[derive(Deserialize, Debug, Default)]
pub struct Config {
    #[serde(default)]
    pub node: NodeConfig,
    #[serde(default)]
    pub mesh: MeshConfig,
    #[serde(default)]
    pub sapient: SapientConfig,
    #[serde(default)]
    pub tak: TakConfig,
}

#[derive(Deserialize, Debug, Default)]
pub struct NodeConfig {
    pub name: Option<String>,
    pub bind: Option<SocketAddr>,
    pub storage: Option<PathBuf>,
}

#[derive(Deserialize, Debug, Default)]
pub struct MeshConfig {
    /// Formation identifier for mesh sync authentication.
    pub formation_id: Option<String>,
    /// Base64-encoded shared formation secret.
    pub shared_key: Option<String>,
    /// Static peers: NODE_ID_HEX@ADDR.
    #[serde(default)]
    pub peers: Vec<String>,
}

#[derive(Deserialize, Debug, Default)]
pub struct SapientConfig {
    /// "hldmm" or "dlmm".
    pub role: Option<String>,
    /// Listen address for HLDMM mode.
    pub listen: Option<SocketAddr>,
    /// Remote HLDMM address for DLMM mode.
    pub remote: Option<SocketAddr>,
    /// Peer node ID for DLMM mode.
    pub peer_node_id: Option<String>,
    /// Use TLS for the SAPIENT connection.
    pub tls: Option<bool>,
    /// Certificate PEM path (server cert in HLDMM mode, client cert in DLMM mode).
    pub cert: Option<PathBuf>,
    /// Private key PEM path.
    pub key: Option<PathBuf>,
    /// CA certificate PEM path for peer verification (mTLS).
    pub ca_cert: Option<PathBuf>,
    /// TLS server name for SNI (DLMM mode only).
    pub tls_server_name: Option<String>,
}

#[derive(Deserialize, Debug, Default)]
pub struct TakConfig {
    /// TAK Server address (host:port).
    pub server: Option<SocketAddr>,
    /// Use TLS for the TAK Server connection.
    pub tls: Option<bool>,
    /// Client certificate PEM path for mTLS.
    pub client_cert: Option<PathBuf>,
    /// Client private key PEM path for mTLS.
    pub client_key: Option<PathBuf>,
    /// CA certificate PEM path for server verification.
    pub ca_cert: Option<PathBuf>,
    /// Callsign for TAK identification.
    pub callsign: Option<String>,
    /// TLS server name for SNI (overrides IP-derived name).
    pub tls_server_name: Option<String>,
    /// Mesh-side node ID for the TAK Server peer.
    pub peer_node_id: Option<String>,
}

impl Config {
    pub fn load(cli: &Cli) -> anyhow::Result<Self> {
        let mut config = if let Some(path) = &cli.config {
            let text = std::fs::read_to_string(path)?;
            toml::from_str(&text)?
        } else {
            Config::default()
        };

        if let Some(name) = &cli.name {
            config.node.name = Some(name.clone());
        }
        if let Some(bind) = cli.bind {
            config.node.bind = Some(bind);
        }
        if let Some(storage) = &cli.storage {
            config.node.storage = Some(storage.clone());
        }
        if !cli.peer.is_empty() {
            config.mesh.peers.extend(cli.peer.iter().cloned());
        }
        if let Some(addr) = cli.sapient_listen {
            config.sapient.role = Some("hldmm".into());
            config.sapient.listen = Some(addr);
        }
        if let Some(addr) = cli.sapient_remote {
            config.sapient.role = Some("dlmm".into());
            config.sapient.remote = Some(addr);
        }
        if let Some(id) = &cli.sapient_peer_id {
            config.sapient.peer_node_id = Some(id.clone());
        }
        if let Some(tls) = cli.sapient_tls {
            config.sapient.tls = Some(tls);
        }
        if let Some(ref path) = cli.sapient_cert {
            config.sapient.cert = Some(path.clone());
        }
        if let Some(ref path) = cli.sapient_key {
            config.sapient.key = Some(path.clone());
        }
        if let Some(ref path) = cli.sapient_ca_cert {
            config.sapient.ca_cert = Some(path.clone());
        }
        if let Some(ref name) = cli.sapient_server_name {
            config.sapient.tls_server_name = Some(name.clone());
        }
        if let Some(addr) = cli.tak_server {
            config.tak.server = Some(addr);
        }
        if let Some(tls) = cli.tak_tls {
            config.tak.tls = Some(tls);
        }
        if let Some(ref path) = cli.tak_cert {
            config.tak.client_cert = Some(path.clone());
        }
        if let Some(ref path) = cli.tak_key {
            config.tak.client_key = Some(path.clone());
        }
        if let Some(ref path) = cli.tak_ca_cert {
            config.tak.ca_cert = Some(path.clone());
        }
        if let Some(ref callsign) = cli.tak_callsign {
            config.tak.callsign = Some(callsign.clone());
        }
        if let Some(ref name) = cli.tak_server_name {
            config.tak.tls_server_name = Some(name.clone());
        }
        config
            .tak
            .peer_node_id
            .get_or_insert(cli.tak_peer_id.clone());

        Ok(config)
    }
}
