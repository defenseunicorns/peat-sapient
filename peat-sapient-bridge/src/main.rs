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
    let use_sapient_tls = config.sapient.tls.unwrap_or(false);

    #[cfg(not(feature = "tls"))]
    if use_sapient_tls {
        anyhow::bail!("sapient.tls = true but binary was compiled without the `tls` feature");
    }

    let transport = {
        let t = PeatSapientTransport::new(sapient_role.clone(), node.clone(), translator.clone());
        #[cfg(feature = "tls")]
        let t = if use_sapient_tls {
            let tls_config = build_sapient_tls(&config.sapient, &sapient_role)?;
            t.with_tls(tls_config, config.sapient.tls_server_name.clone())
        } else {
            t
        };
        t
    };

    let sink = transport.outbound_sink();

    transport.start().await.context("start SAPIENT transport")?;
    info!(
        "sapient: started ({}, tls={})",
        role_summary(&sapient_role),
        use_sapient_tls
    );

    // -- TAK transport (optional) --
    let tak_transport = if let Some(server_addr) = config.tak.server {
        let tak_translator = Arc::new(CotTranslator::new());
        let tak_peer_id = config.tak.peer_node_id.as_deref().unwrap_or("tak-server-0");
        let use_tls = config.tak.tls.unwrap_or(false);
        let identity = if use_tls {
            let client_cert = config
                .tak
                .client_cert
                .clone()
                .ok_or_else(|| anyhow!("TAK TLS requires --tak-cert"))?;
            let client_key = config
                .tak
                .client_key
                .clone()
                .ok_or_else(|| anyhow!("TAK TLS requires --tak-key"))?;
            Some(peat_tak::TakIdentity {
                client_cert,
                client_key,
                ca_cert: config.tak.ca_cert.clone(),
                callsign: config
                    .tak
                    .callsign
                    .clone()
                    .unwrap_or_else(|| "Peat-BRIDGE".into()),
                tls_server_name: config.tak.tls_server_name.clone(),
                credentials: None,
            })
        } else {
            None
        };
        let tak_config = TakMeshConfig {
            server_addr,
            peer_node_id: NodeId::from(tak_peer_id),
            use_tls,
            identity,
        };
        let t = PeatTakTransport::new(tak_config, node.clone(), tak_translator.clone());
        let tak_sink = t.outbound_sink();
        t.start().await.context("start TAK transport")?;
        info!("tak: started (server={}, tls={})", server_addr, use_tls);
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

#[cfg(feature = "tls")]
fn build_sapient_tls(
    cfg: &config::SapientConfig,
    role: &SapientRole,
) -> Result<peat_sapient::connection::SapientTlsConfig> {
    let cert = cfg
        .cert
        .as_ref()
        .ok_or_else(|| anyhow!("SAPIENT TLS requires --sapient-cert"))?;
    let key = cfg
        .key
        .as_ref()
        .ok_or_else(|| anyhow!("SAPIENT TLS requires --sapient-key"))?;

    match role {
        SapientRole::Hldmm { .. } => {
            peat_sapient::connection::SapientTlsConfig::server(cert, key, cfg.ca_cert.as_deref())
                .context("build SAPIENT HLDMM TLS config")
        }
        SapientRole::Dlmm { .. } => {
            let ca = cfg
                .ca_cert
                .as_ref()
                .ok_or_else(|| anyhow!("SAPIENT DLMM TLS requires --sapient-ca-cert"))?;
            peat_sapient::connection::SapientTlsConfig::client(ca, Some(cert), Some(key))
                .context("build SAPIENT DLMM TLS config")
        }
    }
}

fn base64_default_key() -> String {
    use base64::Engine;
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_peer_valid() {
        let id = "a".repeat(64);
        let spec = format!("{id}@127.0.0.1:9001");
        let (parsed_id, addrs) = parse_peer(&spec).unwrap();
        assert_eq!(parsed_id, id);
        assert_eq!(addrs, vec!["127.0.0.1:9001"]);
    }

    #[test]
    fn parse_peer_missing_at() {
        assert!(parse_peer("no-at-sign").is_err());
    }

    #[test]
    fn parse_peer_bad_hex() {
        let spec = format!("{}@127.0.0.1:9001", "zz".repeat(32));
        assert!(parse_peer(&spec).is_err());
    }

    #[test]
    fn parse_peer_wrong_length() {
        let short = "aa".repeat(16);
        let spec = format!("{short}@127.0.0.1:9001");
        let err = parse_peer(&spec).unwrap_err();
        assert!(err.to_string().contains("16 bytes"));
    }

    #[test]
    fn parse_peer_bad_addr() {
        let id = "bb".repeat(32);
        let spec = format!("{id}@not-an-address");
        assert!(parse_peer(&spec).is_err());
    }

    #[test]
    fn resolve_role_default_is_hldmm() {
        let cfg = config::SapientConfig::default();
        let role = resolve_sapient_role(&cfg).unwrap();
        assert!(matches!(role, SapientRole::Hldmm { .. }));
    }

    #[test]
    fn resolve_role_hldmm_explicit() {
        let cfg = config::SapientConfig {
            role: Some("hldmm".into()),
            listen: Some("0.0.0.0:5000".parse().unwrap()),
            ..Default::default()
        };
        match resolve_sapient_role(&cfg).unwrap() {
            SapientRole::Hldmm { listen_addr } => {
                assert_eq!(listen_addr, "0.0.0.0:5000".parse::<SocketAddr>().unwrap())
            }
            _ => panic!("expected HLDMM"),
        }
    }

    #[test]
    fn resolve_role_dlmm() {
        let cfg = config::SapientConfig {
            role: Some("dlmm".into()),
            remote: Some("10.0.0.1:12000".parse().unwrap()),
            ..Default::default()
        };
        assert!(matches!(
            resolve_sapient_role(&cfg).unwrap(),
            SapientRole::Dlmm { .. }
        ));
    }

    #[test]
    fn resolve_role_dlmm_missing_remote() {
        let cfg = config::SapientConfig {
            role: Some("dlmm".into()),
            ..Default::default()
        };
        assert!(resolve_sapient_role(&cfg).is_err());
    }

    #[test]
    fn resolve_role_unknown() {
        let cfg = config::SapientConfig {
            role: Some("bogus".into()),
            ..Default::default()
        };
        assert!(resolve_sapient_role(&cfg).is_err());
    }

    #[test]
    fn config_cli_tls_overrides_file_on() {
        let mut config = Config::default();
        config.sapient.tls = Some(true);
        let cli = Cli::parse_from(["test", "--sapient-tls=false"]);
        let loaded = Config::load(&cli).unwrap();
        assert_eq!(loaded.sapient.tls, Some(false));
    }

    #[test]
    fn config_cli_tls_overrides_file_off() {
        let cli = Cli::parse_from(["test", "--sapient-tls"]);
        let loaded = Config::load(&cli).unwrap();
        assert_eq!(loaded.sapient.tls, Some(true));
    }

    #[test]
    fn config_cli_tak_tls_overrides() {
        let cli = Cli::parse_from(["test", "--tak-tls=false"]);
        let loaded = Config::load(&cli).unwrap();
        assert_eq!(loaded.tak.tls, Some(false));
    }

    #[test]
    fn config_no_tls_flags_leaves_none() {
        let cli = Cli::parse_from(["test"]);
        let loaded = Config::load(&cli).unwrap();
        assert_eq!(loaded.sapient.tls, None);
        assert_eq!(loaded.tak.tls, None);
    }

    #[test]
    fn base64_default_key_is_valid() {
        use base64::Engine;
        let key = base64_default_key();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&key)
            .unwrap();
        assert_eq!(decoded.len(), 32);
        assert_ne!(decoded, vec![0u8; 32]);
    }
}
