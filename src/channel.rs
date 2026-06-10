use tokio::sync::mpsc;

use crate::mux::Command;

use crate::mux::{Frame, MuxError};

pub type ChannelId = u64;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Message {
    /// A packet of data.
    Data(#[serde(with = "serde_bytes")] Vec<u8>),

    /// A message sent by the channel initiator to indicate that the channel should be closed.
    EOF,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ChannelState {
    Open,
    Closed,
}

pub struct ChannelHandle {
    /// The channel's unique identifier.
    channel_id: ChannelId,

    /// A channel for sending frames to the mux task to be forwarded onto the wire.
    frame_tx: mpsc::Sender<Frame>,

    /// A channel for sending commands to the mux task, such as closing the channel.
    command_tx: mpsc::Sender<Command>,

    /// A channel for receiving messages from the mux task that were received from the wire.
    message_rx: mpsc::Receiver<Message>,

    /// The state of the channel.
    state: ChannelState,
}

impl ChannelHandle {
    pub fn new(
        channel_id: ChannelId,
        frame_tx: mpsc::Sender<Frame>,
        command_tx: mpsc::Sender<Command>,
        message_rx: mpsc::Receiver<Message>,
        state: ChannelState,
    ) -> Self {
        ChannelHandle {
            channel_id,
            frame_tx,
            command_tx,
            message_rx,
            state,
        }
    }

    pub async fn send(&self, message: Message) -> Result<(), MuxError> {
        if self.state == ChannelState::Closed {
            return Err(MuxError::ChannelAlreadyClosed(self.channel_id));
        }

        let frame = Frame {
            channel_id: self.channel_id,
            message,
        };
        self.frame_tx
            .send(frame)
            .await
            .map_err(|e| MuxError::MuxTaskTerminated(e.to_string()))
    }

    /// Receives a [`Message`] from the channel.
    /// Returns [`None`] if the channel is closed by peer or the [`Mux`] task has terminated.
    pub async fn recv(&mut self) -> Option<Message> {
        let message = match self.message_rx.recv().await {
            // Message received.
            Some(message) => message,

            // Mux task has terminated.
            None => {
                self.state = ChannelState::Closed;
                return None;
            }
        };

        if let Message::EOF = message {
            self.state = ChannelState::Closed;
        }

        Some(message)
    }

    pub async fn close(&mut self) -> Result<(), MuxError> {
        if self.state == ChannelState::Closed {
            return Ok(());
        }
        self.state = ChannelState::Closed;

        // Notify channel peer that the channel is closing.
        self.command_tx
            .send(Command::CloseChannel {
                channel_id: self.channel_id,
            })
            .await
            .map_err(|e| MuxError::MuxTaskTerminated(e.to_string()))?;

        // Delete channel from the mux's dispatch table.
        self.command_tx
            .send(Command::CloseChannel {
                channel_id: self.channel_id,
            })
            .await
            .map_err(|e| MuxError::MuxTaskTerminated(e.to_string()))?;

        Ok(())
    }
}

impl Drop for ChannelHandle {
    fn drop(&mut self) {
        if self.state == ChannelState::Closed {
            return;
        }
        self.state = ChannelState::Closed;

        // Notify channel peer that the channel is closing.
        let _ = self.frame_tx.try_send(Frame {
            channel_id: self.channel_id,
            message: Message::EOF,
        });

        // Delete channel from the mux's dispatch table.
        let _ = self.command_tx.try_send(Command::CloseChannel {
            channel_id: self.channel_id,
        });
    }
}
