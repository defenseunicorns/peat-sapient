//! Phase 2 TDD: SapientCodec framing over a real in-memory duplex stream.
//!
//! These tests exercise the full encode → TCP bytes → decode path using
//! `tokio::io::duplex`, verifying the 4-byte LE length-prefix protocol.

use bytes::BytesMut;
use futures_util::{SinkExt, StreamExt};
use peat_sapient::{
    codec::{SapientCodec, MAX_FRAME_BYTES},
    proto::{sapient_msg::bsi_flex_335_v2_0::Error as ProtoError, Content, SapientMessage},
};
use prost::Message;
use prost_types::Timestamp;
use tokio_util::codec::{Encoder, FramedRead, FramedWrite};

fn error_msg(node_id: &str, text: &str) -> SapientMessage {
    SapientMessage {
        timestamp: Some(Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        }),
        node_id: Some(node_id.into()),
        content: Some(Content::Error(ProtoError {
            error_message: vec![text.into()],
            ..Default::default()
        })),
        ..Default::default()
    }
}

#[tokio::test]
async fn framed_duplex_single_message() {
    let msg = error_msg("aaaa-1", "hello");

    let (client, server) = tokio::io::duplex(4096);
    let mut writer = FramedWrite::new(client, SapientCodec);
    let mut reader = FramedRead::new(server, SapientCodec);

    writer.send(msg.clone()).await.unwrap();
    drop(writer); // close write side

    let received = reader.next().await.unwrap().unwrap();
    assert_eq!(received, msg);
}

#[tokio::test]
async fn framed_duplex_multiple_messages() {
    let msgs: Vec<SapientMessage> = (0..5)
        .map(|i| error_msg(&format!("node-{i}"), &format!("msg {i}")))
        .collect();

    let (client, server) = tokio::io::duplex(65536);
    let mut writer = FramedWrite::new(client, SapientCodec);
    let mut reader = FramedRead::new(server, SapientCodec);

    for msg in &msgs {
        writer.send(msg.clone()).await.unwrap();
    }
    drop(writer);

    let mut received = Vec::new();
    while let Some(Ok(msg)) = reader.next().await {
        received.push(msg);
    }

    assert_eq!(received, msgs);
}

#[test]
fn wire_format_is_little_endian_length_prefix() {
    let msg = error_msg("le-check", "endian test");
    let payload = msg.encode_to_vec();
    let payload_len = payload.len() as u32;

    let mut codec = SapientCodec;
    let mut buf = BytesMut::new();
    codec.encode(msg, &mut buf).unwrap();

    // First 4 bytes must be a little-endian uint32 equal to payload length.
    let wire_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    assert_eq!(wire_len, payload_len);

    // Verify it is NOT big-endian (unless the length happens to be palindromic).
    let be_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    // Only assert this if the value is large enough that BE ≠ LE.
    if payload_len > 0xFF {
        assert_ne!(be_len, payload_len, "prefix must be little-endian");
    }
}

#[test]
fn oversized_frame_is_rejected() {
    use peat_sapient::SapientError;
    use tokio_util::codec::Decoder;

    let mut codec = SapientCodec;
    let mut buf = BytesMut::new();
    // Write a length prefix 1 byte over the maximum.
    let bad_len = (MAX_FRAME_BYTES + 1) as u32;
    buf.extend_from_slice(&bad_len.to_le_bytes());
    buf.resize(4 + MAX_FRAME_BYTES + 1, 0);

    let result = codec.decode(&mut buf);
    assert!(matches!(result, Err(SapientError::FrameTooLarge { .. })));
}
