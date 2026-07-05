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

    /// Persistence directory. Defaults to a tempdir.
    #[arg(long)]
    pub storage: Option<PathBuf>,

    /// TAK Server address (host:port). Enables TAK transport when set.
    #[arg(long)]
    pub tak_server: Option<SocketAddr>,

    /// Use TLS for the TAK Server connection.
    #[arg(long)]
    pub tak_tls: bool,

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
}

#[derive(Deserialize, Debug, Default)]
pub struct TakConfig {
    /// TAK Server address (host:port).
    pub server: Option<SocketAddr>,
    /// Use TLS for the TAK Server connection.
    pub tls: Option<bool>,
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
        if let Some(addr) = cli.tak_server {
            config.tak.server = Some(addr);
        }
        if cli.tak_tls {
            config.tak.tls = Some(true);
        }
        config
            .tak
            .peer_node_id
            .get_or_insert(cli.tak_peer_id.clone());

        Ok(config)
    }
}
