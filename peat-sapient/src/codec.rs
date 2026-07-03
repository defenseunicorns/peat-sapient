//! TCP framing codec for the SAPIENT wire protocol.
//!
//! SAPIENT uses a 4-byte little-endian uint32 length prefix followed by the
//! serialised `SapientMessage` protobuf body. Confirmed from Dstl Apex middleware
//! `message_io.py` (`struct.pack("<I", len(as_bytes))`).

use bytes::{Buf, BufMut, BytesMut};
use prost::Message;
use tokio_util::codec::{Decoder, Encoder};

use crate::{error::SapientError, proto::SapientMessage};

/// Maximum frame payload accepted. Guards against malformed length prefixes
/// allocating unbounded memory. 16 MiB is well above any realistic SAPIENT message.
pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Codec implementing the SAPIENT 4-byte LE length-prefix framing.
#[derive(Debug, Default, Clone)]
pub struct SapientCodec;

impl Encoder<SapientMessage> for SapientCodec {
    type Error = SapientError;

    fn encode(&mut self, msg: SapientMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let payload = msg.encode_to_vec();
        let len = payload.len() as u32;
        dst.reserve(4 + payload.len());
        dst.put_u32_le(len);
        dst.put_slice(&payload);
        Ok(())
    }
}

impl Decoder for SapientCodec {
    type Item = SapientMessage;
    type Error = SapientError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            src.reserve(4);
            return Ok(None);
        }

        // Peek at the length prefix without consuming it yet.
        let len = u32::from_le_bytes([src[0], src[1], src[2], src[3]]) as usize;

        if len > MAX_FRAME_BYTES {
            return Err(SapientError::FrameTooLarge {
                size: len,
                max: MAX_FRAME_BYTES,
            });
        }

        if src.len() < 4 + len {
            src.reserve(4 + len - src.len());
            return Ok(None);
        }

        src.advance(4);
        let payload = src.split_to(len);
        let msg = SapientMessage::decode(payload)?;
        Ok(Some(msg))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost_types::Timestamp;

    fn test_message() -> SapientMessage {
        SapientMessage {
            timestamp: Some(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            node_id: Some("550e8400-e29b-41d4-a716-446655440000".into()),
            destination_id: None,
            content: None,
            additional_information: None,
        }
    }

    #[test]
    fn encode_produces_le_length_prefix() {
        let mut codec = SapientCodec;
        let msg = test_message();
        let expected_payload = msg.encode_to_vec();

        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).unwrap();

        let prefix = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        assert_eq!(prefix, expected_payload.len());
        assert_eq!(&buf[4..], expected_payload.as_slice());
    }

    #[test]
    fn decode_roundtrip() {
        let mut codec = SapientCodec;
        let msg = test_message();

        let mut buf = BytesMut::new();
        codec.encode(msg.clone(), &mut buf).unwrap();

        let decoded = codec
            .decode(&mut buf)
            .unwrap()
            .expect("should have a message");
        assert_eq!(msg, decoded);
        assert!(buf.is_empty(), "buffer should be fully consumed");
    }

    #[test]
    fn decode_returns_none_on_incomplete_prefix() {
        let mut codec = SapientCodec;
        let mut buf = BytesMut::from(&[0x05, 0x00][..]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn decode_returns_none_on_incomplete_payload() {
        let mut codec = SapientCodec;
        let msg = test_message();
        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).unwrap();

        // Truncate to length prefix + 1 byte of payload
        buf.truncate(5);
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn decode_rejects_oversized_frame() {
        let mut codec = SapientCodec;
        // Write a length prefix claiming MAX + 1 bytes.
        let oversized = (MAX_FRAME_BYTES + 1) as u32;
        let mut buf = BytesMut::new();
        buf.put_u32_le(oversized);
        // Pad enough bytes so the decoder doesn't bail out on incomplete payload.
        buf.put_bytes(0, MAX_FRAME_BYTES + 1);

        let err = codec.decode(&mut buf).unwrap_err();
        assert!(matches!(err, SapientError::FrameTooLarge { .. }));
    }

    #[test]
    fn decode_handles_two_messages_in_one_buffer() {
        let mut codec = SapientCodec;
        let msg = test_message();
        let mut buf = BytesMut::new();
        codec.encode(msg.clone(), &mut buf).unwrap();
        codec.encode(msg.clone(), &mut buf).unwrap();

        let first = codec.decode(&mut buf).unwrap().unwrap();
        let second = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(first, msg);
        assert_eq!(second, msg);
        assert!(buf.is_empty());
    }
}
