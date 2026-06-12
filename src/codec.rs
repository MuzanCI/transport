//! The encoding/decoding protocol for frames sent over the raw TCP connection.
//! Every frame is prefixed with a length header (2 bytes, big-endian).
//! The frame body consists of a [`ChannelId`] (16 bytes, UUID) and a [`Message`] (variable length, up to 64 kilobytes).

use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::channel::{ChannelId, Message};

/// A frame of data that contains a message and the channel that it is belongs to.
/// Frames are sent across the wire and the [`Mux`] task is responsible for dispatching messages to the appropriate channel based on the channel ID in the frame.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Frame {
    /// The channel identifier that this frame belongs to.
    pub channel_id: ChannelId,

    /// The message that is being sent to the peer.
    pub message: Message,
}

const FRAME_LENGTH_HEADER_SIZE: usize = 2; // 2 bytes for payload length
const FRAME_CHANNEL_ID_SIZE: usize = 16; // 16 bytes for channel ID
const FRAME_MESSAGE_MAX_SIZE: usize = 1 << 16; // 64 kilobytes

/// Errors for encoding/decoding frames.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Frame too large: {0}")]
    FrameTooLarge(usize),
    #[error("Channel ID deserialization error: {0}")]
    ChannelIdDeserializationError(#[from] uuid::Error),
    #[error("Message deserialization error: {0}")]
    MessageDeserializationError(postcard::Error),
    #[error("Message serialization error: {0}")]
    MessageSerializationError(postcard::Error),
}

/// A codec that implements [`Encoder`] and [`Decoder`] for [`Frame`].
pub struct Codec;

impl Codec {
    /// Constructs a new [`Codec`].
    pub fn new() -> Self {
        Codec
    }
}

impl Encoder<Frame> for Codec {
    type Error = CodecError;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let message = postcard::to_allocvec(&item.message)
            .map_err(|e| CodecError::MessageSerializationError(e))?;

        if message.len() > FRAME_MESSAGE_MAX_SIZE {
            return Err(CodecError::FrameTooLarge(message.len()));
        }

        dst.reserve(FRAME_LENGTH_HEADER_SIZE + FRAME_CHANNEL_ID_SIZE + message.len());
        dst.put_u16(message.len() as u16);
        dst.put_slice(item.channel_id.as_bytes());
        dst.put_slice(&message);

        Ok(())
    }
}

impl Decoder for Codec {
    type Item = Frame;
    type Error = CodecError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < FRAME_LENGTH_HEADER_SIZE + FRAME_CHANNEL_ID_SIZE {
            // `src` does not have enough bytes to read the length and channel ID.
            // Return `Ok(None)` to wait for more.
            return Ok(None);
        }

        let message_len = u16::from_be_bytes([src[0], src[1]]) as usize;
        if message_len > FRAME_MESSAGE_MAX_SIZE {
            return Err(CodecError::FrameTooLarge(message_len));
        }

        let channel_id = uuid::Uuid::from_slice(
            &src[FRAME_LENGTH_HEADER_SIZE..FRAME_LENGTH_HEADER_SIZE + FRAME_CHANNEL_ID_SIZE],
        )
        .map_err(|e| CodecError::ChannelIdDeserializationError(e))?;

        let total = FRAME_LENGTH_HEADER_SIZE + FRAME_CHANNEL_ID_SIZE + message_len;
        if src.len() < total {
            // `src` does not have enough bytes to read the entire frame.
            // Return `Ok(None)` to wait for more.
            return Ok(None);
        }

        src.advance(FRAME_LENGTH_HEADER_SIZE + FRAME_CHANNEL_ID_SIZE);
        let payload = src.split_to(message_len);
        let message = postcard::from_bytes(&payload)
            .map_err(|e| CodecError::MessageDeserializationError(e))?;

        Ok(Some(Frame {
            channel_id,
            message,
        }))
    }
}
