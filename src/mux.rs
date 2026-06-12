//! A "mux" (multiplexer) allows a single bidirectional data stream (such as a TCP connection)
//! to be used for multiple logical channels.
//!
//! A channel handle can only have one owner.
//!
//! A mux is implemented as a Tokio task that performs the following functions:
//! - Keeps track of open channels and constructs channel handles for other tasks.
//! - Receiving frames from the underlying data stream, decoding them, and forwarding messages to the appropriate channel handle.
//! - Forwards messages from channel handles to the underlying data stream, encoding them as frames.
use std::collections::HashMap;

use futures_util::SinkExt;
use futures_util::StreamExt;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;

use crate::channel::ChannelType;
use crate::{
    channel::{ChannelAcceptor, ChannelHandle, ChannelId, ChannelState, Message},
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

    #[error("Failed to send open channel request: {0}")]
    ChannelOpenRequestFailed(String),

    #[error("Peer failed to open channel: {0}")]
    ChannelOpenPeerFailed(String),
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
        channel_type: ChannelType,
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
}

impl MuxHandle {
    pub async fn open_channel(
        &self,
        channel_type: ChannelType,
        buffer_size: usize,
    ) -> Result<ChannelHandle, MuxError> {
        let channel_id = uuid::Uuid::now_v7();
        let (reply_tx, reply_rx) = oneshot::channel();
        self.command_tx
            .send(Command::OpenChannel {
                channel_id,
                channel_type,
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
            channel_id: uuid::Uuid::nil(),
            message,
        };
        self.frame_tx
            .send(frame)
            .await
            .map_err(|e| MuxError::MuxTaskTerminated(e.to_string()))
    }
}

pub trait TokioStream
where
    Self: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
}

impl<T> TokioStream for T where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static
{
}

struct PendingOpenChannel {
    /// A channel to receive the response from the peer for an open channel request.
    reply: oneshot::Sender<Result<mpsc::Receiver<Message>, MuxError>>,

    /// Pre-allocated channel for messages.
    /// Moved to the mux's dispatch table on peer ack.
    /// Dropped on peer rejection.
    message_tx: mpsc::Sender<Message>,
    message_rx: mpsc::Receiver<Message>,
}

pub struct Mux<Stream, Acceptor>
where
    Stream: TokioStream,
    Acceptor: ChannelAcceptor,
{
    /// The underlying data stream to the peer, such as a TCP connection.
    /// Bytes are read and written with automatic frame encoding and decoding.
    framed: Framed<Stream, Codec>,

    /// A dispatch table mapping channel IDs to channels.
    channels: HashMap<ChannelId, mpsc::Sender<Message>>,

    /// A receiver for inbound [`Frame`]s from other tasks.
    frame_rx: mpsc::Receiver<Frame>,

    /// A receiver for mux [`Command`]s from other tasks.
    command_rx: mpsc::Receiver<Command>,

    /// A mapping of pending open channels that have not yet been acknowledged by the peer.
    pending_open_channels: HashMap<ChannelId, PendingOpenChannel>,

    /// A handler for incoming channel open requests from the peer.
    channel_acceptor: Acceptor,

    /// A sender for mux [`Command`]s to the mux task. Cloned into channel handles.
    command_tx: mpsc::Sender<Command>,

    /// A sender for outbound [`Frame`]s. Cloned into channel handles.
    frame_tx: mpsc::Sender<Frame>,
}

impl<Stream, Acceptor> Mux<Stream, Acceptor>
where
    Stream: TokioStream,
    Acceptor: ChannelAcceptor,
{
    /// Spawns a new mux task for the given stream and returns a handle to the mux.
    pub fn spawn(stream: Stream, channel_acceptor: Acceptor) -> MuxHandle {
        let (frame_tx, frame_rx) = mpsc::channel(100);
        let (command_tx, command_rx) = mpsc::channel(100);

        let mux = Mux {
            framed: Framed::new(stream, Codec::new()),
            channels: HashMap::new(),
            frame_rx,
            command_rx,
            pending_open_channels: HashMap::new(),
            channel_acceptor: channel_acceptor,
            command_tx: command_tx.clone(),
            frame_tx: frame_tx.clone(),
        };

        tokio::spawn(mux.run());

        MuxHandle {
            frame_tx,
            command_tx,
        }
    }

    /// The main loop of the mux task.
    async fn run(mut self) {
        loop {
            tokio::select! {
                // -- Data Outbound: Receive frames from other tasks and forward them onto the wire --
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
                            self.handle_inbound(frame).await;
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

        eprintln!(
            "Mux task terminating, closing {} channels",
            self.channels.len()
        );
    }

    async fn handle_inbound(&mut self, frame: Frame) {
        match frame.message {
            Message::OpenChannelRequest {
                channel_id,
                channel_type,
                buffer_size,
            } => {
                self.handle_peer_open(channel_id, channel_type, buffer_size)
                    .await;
            }
            Message::OpenChannelResponse {
                channel_id,
                result: Ok(()),
            } => {
                self.handle_peer_ok(channel_id).await;
            }
            Message::OpenChannelResponse {
                channel_id,
                result: Err(err),
            } => {
                self.handle_peer_err(channel_id, err).await;
            }
            Message::CloseChannel { channel_id } => {
                self.handle_peer_close(channel_id).await;
            }
            message => {
                self.dispatch_message(frame.channel_id, message).await;
            }
        }
    }

    async fn handle_peer_open(
        &mut self,
        channel_id: ChannelId,
        channel_type: ChannelType,
        buffer_size: usize,
    ) {
        eprintln!("Peer requested to open channel [{}]", channel_id);
        if self.channels.contains_key(&channel_id) {
            eprintln!(
                "Peer requested to open channel with existing channel ID [{}]",
                channel_id
            );

            let frame = Frame {
                channel_id: uuid::Uuid::nil(),
                message: Message::OpenChannelResponse {
                    channel_id,
                    result: Err("Channel ID already exists".to_string()),
                },
            };
            if let Err(e) = self.framed.send(frame).await {
                eprintln!("Failed to send open channel response: {:?}", e);
            }
            return;
        }

        if self.pending_open_channels.contains_key(&channel_id) {
            eprintln!(
                "Peer requested to open channel with pending channel ID [{}]",
                channel_id
            );
            let frame = Frame {
                channel_id: uuid::Uuid::nil(),
                message: Message::OpenChannelResponse {
                    channel_id,
                    result: Err("Channel ID already exists".to_string()),
                },
            };
            if let Err(e) = self.framed.send(frame).await {
                eprintln!("Failed to send open channel response: {:?}", e);
            }
            return;
        }

        let future_fn = match self.channel_acceptor.future_fn(channel_id, channel_type) {
            Ok(future_fn) => future_fn,
            Err(err) => {
                eprintln!("Failed to accept open channel request: {}", err);
                let frame = Frame {
                    channel_id: uuid::Uuid::nil(),
                    message: Message::OpenChannelResponse {
                        channel_id,
                        result: Err(err),
                    },
                };
                if let Err(e) = self.framed.send(frame).await {
                    eprintln!("Failed to send open channel response: {:?}", e);
                }
                return;
            }
        };

        let (message_tx, message_rx) = mpsc::channel(buffer_size);
        self.channels.insert(channel_id, message_tx);

        let frame = Frame {
            channel_id: uuid::Uuid::nil(),
            message: Message::OpenChannelResponse {
                channel_id,
                result: Ok(()),
            },
        };
        if let Err(e) = self.framed.send(frame).await {
            eprintln!("Failed to send open channel ack: {:?}", e);
            self.channels.remove(&channel_id);
            return;
        }

        let channel_handle = ChannelHandle::new(
            channel_id,
            self.frame_tx.clone(),
            self.command_tx.clone(),
            message_rx,
            ChannelState::Open,
        );

        tokio::spawn(future_fn(channel_handle));
    }

    async fn handle_peer_ok(&mut self, channel_id: ChannelId) {
        eprintln!("Peer acknowledged open channel [{}]", channel_id);
        match self.pending_open_channels.remove(&channel_id) {
            Some(pending) => {
                self.channels.insert(channel_id, pending.message_tx);
                let _ = pending.reply.send(Ok(pending.message_rx));
            }
            None => {
                eprintln!(
                    "Received open channel ack for unknown channel ID [{}]",
                    channel_id
                );
            }
        }
    }

    async fn handle_peer_err(&mut self, channel_id: ChannelId, err: String) {
        eprintln!("Peer failed to open channel [{}]: {}", channel_id, err);
        match self.pending_open_channels.remove(&channel_id) {
            Some(pending) => {
                let _ = pending
                    .reply
                    .send(Err(MuxError::ChannelOpenPeerFailed(err)));
            }
            None => {
                eprintln!(
                    "Received open channel error for unknown channel ID [{}]",
                    channel_id
                );
            }
        }
    }

    async fn handle_peer_close(&mut self, channel_id: ChannelId) {
        eprintln!("Peer closed channel [{}]", channel_id);
        if self.channels.remove(&channel_id).is_none() {
            eprintln!(
                "Received close channel for unknown channel ID [{}]",
                channel_id
            );
        }
    }

    async fn dispatch_message(&mut self, channel_id: ChannelId, message: Message) {
        match self.channels.get(&channel_id) {
            Some(channel) => {
                if let Err(e) = channel.send(message).await {
                    eprintln!(
                        "Failed to send message to channel [{}]: {:?}",
                        channel_id, e
                    );
                }
            }
            None => {
                eprintln!("Received frame for unknown channel ID [{}]", channel_id);
            }
        }
    }

    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::OpenChannel {
                channel_id,
                channel_type,
                buffer_size,
                reply,
            } => {
                if self.channels.contains_key(&channel_id) {
                    let _ = reply.send(Err(MuxError::ChannelIdAlreadyExists(channel_id)));
                    return;
                }

                if self.pending_open_channels.contains_key(&channel_id) {
                    let _ = reply.send(Err(MuxError::ChannelIdAlreadyExists(channel_id)));
                    return;
                }

                let (message_tx, message_rx) = mpsc::channel(buffer_size);
                self.pending_open_channels.insert(
                    channel_id,
                    PendingOpenChannel {
                        reply,
                        message_tx,
                        message_rx,
                    },
                );

                let frame = Frame {
                    channel_id: uuid::Uuid::nil(),
                    message: Message::OpenChannelRequest {
                        channel_id,
                        channel_type,
                        buffer_size,
                    },
                };
                if let Err(e) = self.framed.send(frame).await {
                    eprintln!("Failed to send open channel request: {:?}", e);
                    if let Some(pending) = self.pending_open_channels.remove(&channel_id) {
                        let _ = pending
                            .reply
                            .send(Err(MuxError::ChannelOpenRequestFailed(e.to_string())));
                    }
                }
            }
            Command::CloseChannel { channel_id } => {
                self.channels.remove(&channel_id);
                let frame = Frame {
                    channel_id: uuid::Uuid::nil(),
                    message: Message::CloseChannel { channel_id },
                };
                if let Err(e) = self.framed.send(frame).await {
                    eprintln!("Failed to send close channel request: {:?}", e);
                }
            }
        }
    }
}

impl<Stream, Acceptor> Drop for Mux<Stream, Acceptor>
where
    Stream: TokioStream,
    Acceptor: ChannelAcceptor,
{
    /// The mux is dropped when the mux task terminates.
    /// This causes all ChannelHandle::recv() tasks wake up and returns None.
    fn drop(&mut self) {
        eprintln!("Mux dropped, closing {} channels", self.channels.len());
    }
}
