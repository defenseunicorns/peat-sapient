//! Integration tests for SAPIENT TLS (mTLS) connections.
//!
//! Uses rcgen to generate self-signed CA, server, and client certs, then
//! verifies that `connect_tls` / `accept_tls` complete a handshake and
//! exchange SAPIENT-framed protobuf messages over the encrypted link.

#![cfg(feature = "tls")]

use std::io::Write;
use std::path::PathBuf;

use peat_sapient::connection::{self, SapientTlsConfig};
use peat_sapient::proto::{Content, DetectionReport, SapientMessage};
use tokio::net::TcpListener;

struct TestCerts {
    ca_cert: PathBuf,
    server_cert: PathBuf,
    server_key: PathBuf,
    client_cert: PathBuf,
    client_key: PathBuf,
}

fn generate_test_certs(tmp: &std::path::Path) -> TestCerts {
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "Test CA");
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let ca_issuer = rcgen::Issuer::from_params(&ca_params, &ca_key);

    let mut server_params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    server_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "SAPIENT HLDMM");
    server_params
        .subject_alt_names
        .push(rcgen::SanType::IpAddress("127.0.0.1".parse().unwrap()));
    let server_key = rcgen::KeyPair::generate().unwrap();
    let server_cert = server_params.signed_by(&server_key, &ca_issuer).unwrap();

    let mut client_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    client_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "SAPIENT DLMM");
    let client_key = rcgen::KeyPair::generate().unwrap();
    let client_cert = client_params.signed_by(&client_key, &ca_issuer).unwrap();

    let paths = TestCerts {
        ca_cert: tmp.join("ca.pem"),
        server_cert: tmp.join("server.pem"),
        server_key: tmp.join("server-key.pem"),
        client_cert: tmp.join("client.pem"),
        client_key: tmp.join("client-key.pem"),
    };

    std::fs::File::create(&paths.ca_cert)
        .unwrap()
        .write_all(ca_cert.pem().as_bytes())
        .unwrap();
    std::fs::File::create(&paths.server_cert)
        .unwrap()
        .write_all(server_cert.pem().as_bytes())
        .unwrap();
    std::fs::File::create(&paths.server_key)
        .unwrap()
        .write_all(server_key.serialize_pem().as_bytes())
        .unwrap();
    std::fs::File::create(&paths.client_cert)
        .unwrap()
        .write_all(client_cert.pem().as_bytes())
        .unwrap();
    std::fs::File::create(&paths.client_key)
        .unwrap()
        .write_all(client_key.serialize_pem().as_bytes())
        .unwrap();

    paths
}

fn make_test_message(node_id: &str, object_id: &str) -> SapientMessage {
    SapientMessage {
        node_id: Some(node_id.to_string()),
        content: Some(Content::DetectionReport(DetectionReport {
            report_id: Some("rpt-001".to_string()),
            object_id: Some(object_id.to_string()),
            ..Default::default()
        })),
        ..Default::default()
    }
}

#[tokio::test]
async fn tls_handshake_and_sapient_message_exchange() {
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();

    let tmp = tempfile::tempdir().unwrap();
    let certs = generate_test_certs(tmp.path());

    let server_tls =
        SapientTlsConfig::server(&certs.server_cert, &certs.server_key, Some(&certs.ca_cert))
            .expect("server TLS config");

    let client_tls = SapientTlsConfig::client(
        &certs.ca_cert,
        Some(&certs.client_cert),
        Some(&certs.client_key),
    )
    .expect("client TLS config");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        let (mut framed, peer_addr) = connection::accept_tls(&listener, &server_tls)
            .await
            .expect("accept_tls");
        assert!(peer_addr.ip().is_loopback());

        let msg = connection::recv(&mut framed)
            .await
            .expect("recv")
            .expect("stream closed");
        assert_eq!(msg.node_id.as_deref(), Some("dlmm-001"));
        if let Some(Content::DetectionReport(dr)) = &msg.content {
            assert_eq!(dr.object_id.as_deref(), Some("track-from-dlmm"));
        } else {
            panic!("expected DetectionReport");
        }

        let reply = make_test_message("hldmm-001", "track-from-hldmm");
        connection::send(&mut framed, reply).await.expect("send");
    });

    let mut client_framed = connection::connect_tls(server_addr, &client_tls, "localhost")
        .await
        .expect("connect_tls");

    let outbound = make_test_message("dlmm-001", "track-from-dlmm");
    connection::send(&mut client_framed, outbound)
        .await
        .expect("send");

    let reply = connection::recv(&mut client_framed)
        .await
        .expect("recv")
        .expect("stream closed");
    assert_eq!(reply.node_id.as_deref(), Some("hldmm-001"));
    if let Some(Content::DetectionReport(dr)) = &reply.content {
        assert_eq!(dr.object_id.as_deref(), Some("track-from-hldmm"));
    } else {
        panic!("expected DetectionReport in reply");
    }

    server_handle.await.unwrap();
}

#[tokio::test]
async fn tls_mtls_rejects_untrusted_client() {
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();

    let tmp = tempfile::tempdir().unwrap();
    let certs = generate_test_certs(tmp.path());

    let server_tls =
        SapientTlsConfig::server(&certs.server_cert, &certs.server_key, Some(&certs.ca_cert))
            .expect("server TLS config");

    let no_mtls_client =
        SapientTlsConfig::client(&certs.ca_cert, None, None).expect("client TLS config");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        let result = connection::accept_tls(&listener, &server_tls).await;
        assert!(
            result.is_err(),
            "mTLS server should reject unauthenticated client"
        );
    });

    // With TLS 1.3, the client handshake may complete before the server
    // rejects the missing client cert. The failure surfaces on the first
    // send or recv instead.
    let connect_result = connection::connect_tls(server_addr, &no_mtls_client, "localhost").await;
    match connect_result {
        Err(_) => {} // handshake failed — expected
        Ok(mut framed) => {
            let msg = make_test_message("no-cert", "should-fail");
            let send_result = connection::send(&mut framed, msg).await;
            let recv_result = connection::recv(&mut framed).await;
            assert!(
                send_result.is_err() || matches!(recv_result, Ok(None) | Err(_)),
                "communication must fail without client cert"
            );
        }
    }

    server_handle.await.unwrap();
}

#[tokio::test]
async fn tls_server_only_no_mtls() {
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();

    let tmp = tempfile::tempdir().unwrap();
    let certs = generate_test_certs(tmp.path());

    let server_tls = SapientTlsConfig::server(&certs.server_cert, &certs.server_key, None)
        .expect("server TLS config");

    let client_tls =
        SapientTlsConfig::client(&certs.ca_cert, None, None).expect("client TLS config");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        let (mut framed, _) = connection::accept_tls(&listener, &server_tls)
            .await
            .expect("accept_tls without mTLS");

        let msg = connection::recv(&mut framed)
            .await
            .expect("recv")
            .expect("stream closed");
        assert_eq!(msg.node_id.as_deref(), Some("dlmm-001"));
    });

    let mut client_framed = connection::connect_tls(server_addr, &client_tls, "localhost")
        .await
        .expect("connect_tls without mTLS");

    let msg = make_test_message("dlmm-001", "server-only-tls");
    connection::send(&mut client_framed, msg)
        .await
        .expect("send over server-only TLS");

    server_handle.await.unwrap();
}
