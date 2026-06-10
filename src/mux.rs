//! A "mux" (multiplexer) allows a single bidirectional data stream (such as a TCP connection)
//! to be used for multiple logical channels.
//!
//! A channel handle can only have one owner.
//!
//! A mux is implemented as a Tokio task that performs the following functions:
//! - Keeps track of open channels and constructs channel handles for other tasks.
//! - Receiving frames from the underlying data stream, decoding them, and forwarding messages to the appropriate channel handle.
//! - Forwards messages from channel handles to the underlying data stream, encoding them as frames.
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use futures_util::SinkExt;
use futures_util::StreamExt;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;

use crate::{
    channel::{ChannelHandle, ChannelId, ChannelState, Message},
    codec::Codec,
};

#[derive(Debug, thiserror::Error)]
pub enum MuxError {
    #[error("Channel ID [{0}] not found")]
    ChannelIdNotFound(ChannelId),

    #[error("Channel ID [{0}] already exists")]
    ChannelIdAlreadyExists(ChannelId),

    #[error("Channel ID [{0}] already closed")]
    ChannelAlreadyClosed(ChannelId),

    #[error("Mux task terminated: {0}")]
    MuxTaskTerminated(String),
}

/// A frame of data that contains a message and the channel that it is belongs to.
/// Frames are sent across the wire and the [`Mux`] task is responsible for dispatching messages to the appropriate channel based on the channel ID in the frame.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Frame {
    /// The ID of the channel this frame belongs to.
    pub channel_id: ChannelId,

    /// The message payload.
    pub message: Message,
}

/// A command sent to the mux to perform an action, such as opening a channel.
/// Commands must be async and replies are sent through a channel, since the mux runs in a single thread and cannot block on a command.
pub enum Command {
    /// A command for the mux to open a channel.
    /// Upon success, the mux will reply with a `MessageReceiver` for the channel.
    /// Otherwise, the mux will reply with an error.
    OpenChannel {
        channel_id: ChannelId,
        buffer_size: usize,
        reply: oneshot::Sender<Result<mpsc::Receiver<Message>, MuxError>>,
    },

    /// A command for the mux to close a channel.
    CloseChannel { channel_id: ChannelId },
}

/// A handle to a mux.
#[derive(Clone)]
pub struct MuxHandle {
    /// Sends frames onto the wire.
    frame_tx: mpsc::Sender<Frame>,

    /// Sends commands to the mux task.
    command_tx: mpsc::Sender<Command>,

    /// Monotonically increasing ID generator for channels.
    next_channel_id: Arc<AtomicU64>,
}

impl MuxHandle {
    pub async fn open_channel(&self, buffer_size: usize) -> Result<ChannelHandle, MuxError> {
        let channel_id = self
            .next_channel_id
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1); // Channel ID 0 is reserved for control messages.

        let (reply_tx, reply_rx) = oneshot::channel();

        self.command_tx
            .send(Command::OpenChannel {
                channel_id,
                buffer_size,
                reply: reply_tx,
            })
            .await
            .map_err(|e| MuxError::MuxTaskTerminated(e.to_string()))?;

        let message_rx = reply_rx
            .await
            .map_err(|e| MuxError::MuxTaskTerminated(e.to_string()))??;

        Ok(ChannelHandle::new(
            channel_id,
            self.frame_tx.clone(),
            self.command_tx.clone(),
            message_rx,
            ChannelState::Open,
        ))
    }

    pub async fn send_control(&self, message: Message) -> Result<(), MuxError> {
        let frame = Frame {
            channel_id: 0,
            message,
        };
        self.frame_tx
            .send(frame)
            .await
            .map_err(|e| MuxError::MuxTaskTerminated(e.to_string()))
    }
}

pub trait ThreadSafeStream
where
    Self: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
}

impl<T> ThreadSafeStream for T where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static
{
}

pub struct Mux<Stream>
where
    Stream: ThreadSafeStream,
{
    /// The underlying data stream to the peer, such as a TCP connection.
    /// Bytes are read and written with automatic frame encoding and decoding.
    framed: Framed<Stream, Codec>,

    /// A receiver for [`Frame`]s from other tasks.
    /// Frames will be forwarded onto the wire.
    frame_rx: mpsc::Receiver<Frame>,

    /// A receiver for mux [`Command`]s from other tasks.
    command_rx: mpsc::Receiver<Command>,

    /// A dispatch table mapping channel IDs to channels.
    channels: HashMap<ChannelId, mpsc::Sender<Message>>,
}

impl<Stream> Mux<Stream>
where
    Stream: ThreadSafeStream,
{
    /// Spawns a new mux task for the given stream and returns a handle to the mux.
    pub fn spawn(stream: Stream) -> MuxHandle {
        let (frame_tx, frame_rx) = mpsc::channel(100);
        let (command_tx, command_rx) = mpsc::channel(100);

        let mux = Mux {
            framed: Framed::new(stream, Codec::new()),
            frame_rx,
            command_rx,
            channels: HashMap::new(),
        };

        tokio::spawn(mux.run());

        MuxHandle {
            frame_tx,
            command_tx,
            next_channel_id: Arc::new(AtomicU64::new(0)),
        }
    }

    /// The main loop of the mux task.
    async fn run(mut self) {
        loop {
            tokio::select! {
                // -- Data Outbound: Receive frames from other tasks and forward them onto the wire --
                biased;
                maybe_frame = self.frame_rx.recv() => {
                    match maybe_frame {
                        // Forward frame onto the wire.
                        Some(frame) => {
                            if let Err(e) = self.framed.send(frame).await {
                                eprintln!("Failed to send frame: {:?}", e);
                                break;
                            }
                        }

                        // All senders have been dropped. Terminate the mux task.
                        None => {
                            break;
                        }
                    }
                }

                // -- Data Inbound: Handle frames from the wire and dispatch them to the appropriate channel --
                maybe_frame = self.framed.next() => {
                    match maybe_frame {
                        // Dispatch message to appropriate channel.
                        Some(Ok(frame)) => {
                            self.dispatch_frame(frame).await;
                        }

                        // An error occurred while reading from the stream or decoding a frame.
                        Some(Err(e)) => {
                            eprintln!("Failed to read frame: {:?}", e);
                            break;
                        }

                        // The peer has closed the connection. Terminate the mux task.
                        None => {
                            break;
                        }
                    }
                }

                // -- Command Inbound: Handle commands from other tasks, such as opening and closing channels --
                maybe_command = self.command_rx.recv() => {
                    match maybe_command {
                        // Handle command.
                        Some(command) => {
                            self.handle_command(command).await;
                        }
                        // All mux handles have been dropped. Terminate the mux task.
                        None => {
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn dispatch_frame(&mut self, frame: Frame) {
        if let Some(channel) = self.channels.get(&frame.channel_id) {
            if let Err(e) = channel.send(frame.message).await {
                eprintln!(
                    "Failed to send message to channel [{}]: {:?}",
                    frame.channel_id, e
                );
            }
        } else {
            eprintln!(
                "Received frame for unknown channel ID [{}]",
                frame.channel_id
            );
        }
    }

    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::OpenChannel {
                channel_id,
                buffer_size,
                reply,
            } => {
                if self.channels.contains_key(&channel_id) {
                    let _ = reply.send(Err(MuxError::ChannelIdAlreadyExists(channel_id)));
                    return;
                }

                let (message_tx, message_rx) = mpsc::channel(buffer_size);
                self.channels.insert(channel_id, message_tx);
                let _ = reply.send(Ok(message_rx));
            }
            Command::CloseChannel { channel_id } => {
                self.channels.remove(&channel_id);
            }
        }
    }
}

impl<Stream> Drop for Mux<Stream>
where
    Stream: ThreadSafeStream,
{
    /// The mux is dropped when the mux task terminates.
    /// This causes all ChannelHandle::recv() tasks wake up and returns None.
    fn drop(&mut self) {
        eprintln!("Mux dropped, closing {} channels", self.channels.len());
    }
}
