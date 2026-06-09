pub const MUZANCI_TRANSPORT_V1: &str = "muzanci-transport/v1";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Frame {
    pub channel_id: u16,
    pub message: Message,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Message {
    OpenChannel { channel_id: u16 },
    CloseChannel { channel_id: u16 },
    Ping,
    Data(#[serde(with = "serde_bytes")] Vec<u8>),
}

// ----- mux/mod.rs -----
pub trait ChannelRegistry: Send + Sync {
    fn insert(&self, channel_id: u16, tx: tokio::sync::mpsc::Sender<Message>);
    fn remove(&self, channel_id: u16);
    fn get(&self, channel_id: u16) -> Option<tokio::sync::mpsc::Sender<Message>>;
}

/// A handle to a single logical channel.
pub struct ChannelHandle {
    pub id: u16,
    tx: tokio::sync::mpsc::Sender<Message>,
    rx: tokio::sync::mpsc::Receiver<Message>,
}

pub struct MuxCodec;

impl MuxCodec {
    pub fn encode(frame: Frame) -> Vec<u8> {
        unimplemented!();
    }

    // TODO: Define library errors with thiserror.
    pub fn decode(bytes: &[u8]) -> std::io::Result<Frame> {
        unimplemented!();
    }
}
