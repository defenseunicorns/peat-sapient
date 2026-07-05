mod config;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use peat_mesh::sync::automerge_backend::{AutomergeBackend, AutomergeBackendConfig};
use peat_mesh::sync::{DataSyncBackend, SyncEngine};
use peat_mesh::transport::{
    MeshTransport, NodeId, TranslatorRegistrationConfig, TransportManager, TransportManagerConfig,
};
use peat_mesh::Node;
use peat_mesh_sapient::{PeatSapientTransport, SapientRole, SapientTranslator};
use peat_tak::{CotTranslator, PeatTakTransport, TakMeshConfig};
use tracing::{error, info};

use crate::config::{Cli, Config};

fn parse_peer(spec: &str) -> Result<(String, Vec<String>)> {
    let (id_hex, addr_str) = spec
        .split_once('@')
        .ok_or_else(|| anyhow!("--peer needs NODE_ID_HEX@ADDR (64 hex chars @ ip:port)"))?;
    let raw =
        hex::decode(id_hex).with_context(|| format!("node id {id_hex:?} is not valid hex"))?;
    if raw.len() != 32 {
        return Err(anyhow!(
            "node id has {} bytes, need 32 (64 hex chars)",
            raw.len()
        ));
    }
    let _: SocketAddr = addr_str
        .parse()
        .with_context(|| format!("{addr_str:?} is not a valid IP:PORT"))?;
    Ok((id_hex.to_string(), vec![addr_str.to_string()]))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "warn,peat_sapient_bridge=info,peat_mesh_sapient=info,peat_sapient=info,peat_tak=info".into()
            }),
        )
        .init();

    let cli = Cli::parse();
    let config = Config::load(&cli)?;

    let node_name = config.node.name.as_deref().unwrap_or("sapient-bridge");

    // -- Storage --
    let (data_dir, _tempdir_guard) = match &config.node.storage {
        Some(p) => (p.clone(), None),
        None => {
            let td = tempfile::tempdir()?;
            (td.path().to_path_buf(), Some(td))
        }
    };

    // -- Mesh backend (Automerge + iroh) --
    let formation_id = config
        .mesh
        .formation_id
        .clone()
        .unwrap_or_else(|| "sapient-bridge-default".into());
    let shared_key = config
        .mesh
        .shared_key
        .clone()
        .unwrap_or_else(base64_default_key);

    let mut backend_config = AutomergeBackendConfig::default();
    backend_config.data_dir = data_dir.clone();
    backend_config.formation_id = formation_id.clone();
    backend_config.base64_shared_key = shared_key;
    backend_config.iroh_bind_addr = config.node.bind;

    let backend = AutomergeBackend::with_iroh(backend_config)
        .await
        .context("create Automerge+iroh backend")?;

    let endpoint_id = backend.blob_store().endpoint_id();
    let endpoint_id_hex = hex::encode(endpoint_id.as_bytes());

    backend.start_sync().await.context("start mesh sync")?;

    info!(
        "mesh node '{}' ready (id={} bind={:?})",
        node_name,
        &endpoint_id_hex[..16],
        config.node.bind,
    );
    if let Some(bind) = config.node.bind {
        info!("  reach me with: --peer {}@{}", endpoint_id_hex, bind);
    }

    let node = Arc::new(Node::new(backend.clone() as Arc<dyn DataSyncBackend>));

    // -- Static mesh peers --
    for spec in &config.mesh.peers {
        match parse_peer(spec) {
            Ok((id_hex, addrs)) => {
                let backend_ref = backend.clone();
                tokio::spawn(async move {
                    loop {
                        match backend_ref.connect_to_peer(&id_hex, &addrs).await {
                            Ok(true) => {
                                info!("mesh: connected to peer {}…", &id_hex[..16]);
                                break;
                            }
                            Ok(false) => {}
                            Err(e) => {
                                tracing::debug!("mesh: dial {}… failed: {}", &id_hex[..16], e);
                            }
                        }
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                });
            }
            Err(e) => error!("bad --peer {spec:?}: {e}"),
        }
    }

    // -- SAPIENT transport --
    let translator = Arc::new(SapientTranslator::new());

    let sapient_role = resolve_sapient_role(&config.sapient)?;
    let transport =
        PeatSapientTransport::new(sapient_role.clone(), node.clone(), translator.clone());
    let sink = transport.outbound_sink();

    transport.start().await.context("start SAPIENT transport")?;
    info!("sapient: started ({:?})", role_summary(&sapient_role));

    // -- TAK transport (optional) --
    let tak_transport = if let Some(server_addr) = config.tak.server {
        let tak_translator = Arc::new(CotTranslator::new());
        let tak_peer_id = config.tak.peer_node_id.as_deref().unwrap_or("tak-server-0");
        let tak_config = TakMeshConfig {
            server_addr,
            peer_node_id: NodeId::from(tak_peer_id),
            use_tls: config.tak.tls.unwrap_or(false),
        };
        let t = PeatTakTransport::new(tak_config, node.clone(), tak_translator.clone());
        let tak_sink = t.outbound_sink();
        t.start().await.context("start TAK transport")?;
        info!("tak: started (server={})", server_addr);
        Some((t, tak_translator, tak_sink))
    } else {
        None
    };

    // -- TransportManager fan-out --
    let mgr = Arc::new(TransportManager::new(TransportManagerConfig::default()));
    mgr.register_translator(translator, sink, TranslatorRegistrationConfig::default())
        .await
        .context("register SAPIENT translator")?;
    if let Some((_, ref tak_translator, ref tak_sink)) = tak_transport {
        mgr.register_translator(
            tak_translator.clone(),
            tak_sink.clone(),
            TranslatorRegistrationConfig::default(),
        )
        .await
        .context("register TAK translator")?;
    }
    let _fanout_handle = mgr
        .start_fanout(node.clone(), vec!["tracks".to_string()])
        .context("start fan-out")?;
    info!("fan-out: observing 'tracks' collection");

    // -- Wait for shutdown --
    tokio::signal::ctrl_c().await?;
    info!("shutting down");
    if let Some((ref tak, _, _)) = tak_transport {
        tak.stop().await.ok();
    }
    transport.stop().await.ok();

    Ok(())
}

fn resolve_sapient_role(cfg: &config::SapientConfig) -> Result<SapientRole> {
    match cfg.role.as_deref() {
        Some("hldmm") | None => {
            let listen = cfg
                .listen
                .unwrap_or_else(|| "0.0.0.0:12000".parse().unwrap());
            Ok(SapientRole::Hldmm {
                listen_addr: listen,
            })
        }
        Some("dlmm") => {
            let remote = cfg
                .remote
                .ok_or_else(|| anyhow!("DLMM mode requires sapient.remote address"))?;
            let peer_node_id = cfg.peer_node_id.as_deref().unwrap_or("hldmm-0");
            Ok(SapientRole::Dlmm {
                remote_addr: remote,
                peer_node_id: NodeId::from(peer_node_id),
            })
        }
        Some(other) => Err(anyhow!(
            "unknown sapient.role: {other:?} (expected hldmm or dlmm)"
        )),
    }
}

fn role_summary(role: &SapientRole) -> String {
    match role {
        SapientRole::Hldmm { listen_addr } => format!("HLDMM listening on {listen_addr}"),
        SapientRole::Dlmm {
            remote_addr,
            peer_node_id,
        } => format!("DLMM connecting to {remote_addr} as {peer_node_id}"),
    }
}

fn base64_default_key() -> String {
    use std::io::Read;
    let mut bytes = [0u8; 32];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        let _ = f.read_exact(&mut bytes);
    }
    base64_encode(&bytes)
}

fn base64_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 4 / 3 + 4);
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        let _ = s.write_char(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        let _ = s.write_char(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            let _ = s.write_char(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            let _ = s.write_char('=');
        }
        if chunk.len() > 2 {
            let _ = s.write_char(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            let _ = s.write_char('=');
        }
    }
    s
}
