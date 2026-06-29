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
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use futures_util::SinkExt;
use futures_util::StreamExt;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;

use crate::channel::ChannelType;
use crate::channel::ControlMessage;
use crate::channel::channel;
use crate::codec::Frame;
use crate::{
    channel::{ChannelAcceptor, ChannelId, ChannelReceiver, ChannelSender, Message},
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

struct OpenChannelCommandResult {
    message_rx: mpsc::Receiver<Message>,
    closed: Arc<AtomicBool>,
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
        // [4] Convert to "Peer Channel" oneshot sender.
        reply: oneshot::Sender<Result<OpenChannelCommandResult, MuxError>>,
    },

    /// A command for the mux to close a channel.
    CloseChannel { channel_id: ChannelId },
}

struct PeerChannel {
    message_tx: mpsc::Sender<Message>,
    closed: Arc<AtomicBool>,
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
    ) -> Result<(ChannelSender, ChannelReceiver), MuxError> {
        tracing::info!("Requesting to open channel of type [{:?}]", channel_type);
        let channel_id = uuid::Uuid::now_v7();
        let (reply_tx, reply_rx) = oneshot::channel();
        self.command_tx
            .send(Command::OpenChannel {
                channel_id,
                channel_type,
                reply: reply_tx, // [3] Convert to "Peer Channel" oneshot sender.
            })
            .await
            .map_err(|e| MuxError::MuxTaskTerminated(e.to_string()))?;

        // [2] Convert to "Peer Channel".
        let OpenChannelCommandResult { message_rx, closed } = reply_rx
            .await
            .map_err(|e| MuxError::MuxTaskTerminated(e.to_string()))??;

        Ok(channel(
            channel_id,
            channel_type,
            self.frame_tx.clone(),
            self.command_tx.clone(),
            message_rx,
            closed,
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

struct PendingOpenChannelCommand {
    /// A channel to receive the response from the peer for an open channel request.
    // [6] Convert to "Peer Channel" oneshot sender.
    reply: oneshot::Sender<Result<OpenChannelCommandResult, MuxError>>,
}

pub struct Mux<Stream, Acceptor>
where
    Stream: TokioStream,
    Acceptor: ChannelAcceptor,
{
    /// The underlying data stream to the peer, such as a TCP connection.
    /// Bytes are read and written with automatic frame encoding and decoding.
    framed: Framed<Stream, Codec>,

    /// A dispatch table mapping channel IDs to peer channels.
    peer_channels: HashMap<ChannelId, PeerChannel>,

    /// A receiver for inbound [`Frame`]s from other tasks.
    frame_rx: mpsc::Receiver<Frame>,

    /// A receiver for mux [`Command`]s from other tasks.
    command_rx: mpsc::Receiver<Command>,

    /// A mapping of pending open channels that have not yet been acknowledged by the peer.
    pending_open_channel_commands: HashMap<ChannelId, PendingOpenChannelCommand>,

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
            peer_channels: HashMap::new(),
            frame_rx,
            command_rx,
            pending_open_channel_commands: HashMap::new(),
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
                                tracing::error!("Failed to send frame: {:?}", e);
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
                            tracing::error!("Failed to read frame: {:?}", e);
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

        tracing::warn!(
            "Mux task terminating, closing {} channels",
            self.peer_channels.len()
        );
    }

    async fn handle_inbound(&mut self, frame: Frame) {
        if frame.channel_id.is_nil() {
            // Message for control channel.
            match frame.message {
                Message::Control(ControlMessage::OpenChannelRequest {
                    channel_id,
                    channel_type,
                }) => {
                    self.handle_peer_open(channel_id, channel_type).await;
                }
                Message::Control(ControlMessage::OpenChannelResponse {
                    channel_id,
                    result: Ok(()),
                }) => {
                    self.handle_peer_ok(channel_id).await;
                }
                Message::Control(ControlMessage::OpenChannelResponse {
                    channel_id,
                    result: Err(err),
                }) => {
                    self.handle_peer_err(channel_id, err).await;
                }
                Message::Control(ControlMessage::CloseChannel { channel_id }) => {
                    self.handle_peer_close(channel_id).await;
                }
                _ => {
                    panic!(
                        "Received unexpected message on the control channel: {:?}",
                        frame.message
                    );
                }
            }
        } else {
            // Message for non-control channel.
            self.dispatch_message(frame.channel_id, frame.message).await;
        }
    }

    async fn handle_peer_open(&mut self, channel_id: ChannelId, channel_type: ChannelType) {
        tracing::info!(
            "Received request to open channel type [{:?}] with ID [{}]",
            channel_type,
            channel_id
        );
        if self.peer_channels.contains_key(&channel_id) {
            tracing::error!(
                "Peer requested to open channel with existing channel ID [{}]",
                channel_id
            );

            let frame = Frame {
                channel_id: uuid::Uuid::nil(),
                message: Message::Control(ControlMessage::OpenChannelResponse {
                    channel_id,
                    result: Err("Channel ID already exists".to_string()),
                }),
            };
            if let Err(e) = self.framed.send(frame).await {
                tracing::error!("Failed to send open channel response: {:?}", e);
            }
            return;
        }

        if self.pending_open_channel_commands.contains_key(&channel_id) {
            tracing::error!(
                "Peer requested to open channel with pending channel ID [{}]",
                channel_id
            );
            let frame = Frame {
                channel_id: uuid::Uuid::nil(),
                message: Message::Control(ControlMessage::OpenChannelResponse {
                    channel_id,
                    result: Err("Channel ID already exists".to_string()),
                }),
            };
            if let Err(e) = self.framed.send(frame).await {
                tracing::error!("Failed to send open channel response: {:?}", e);
            }
            return;
        }

        let future_fn = match self.channel_acceptor.future_fn(channel_id, channel_type) {
            Ok(future_fn) => future_fn,
            Err(err) => {
                tracing::error!("Failed to accept open channel request: {}", err);
                let frame = Frame {
                    channel_id: uuid::Uuid::nil(),
                    message: Message::Control(ControlMessage::OpenChannelResponse {
                        channel_id,
                        result: Err(err),
                    }),
                };
                if let Err(e) = self.framed.send(frame).await {
                    tracing::error!("Failed to send open channel response: {:?}", e);
                }
                return;
            }
        };

        // TODO: Consider parameterizing the buffer size.
        let (message_tx, message_rx) = mpsc::channel(1);
        let closed = Arc::new(AtomicBool::new(false));
        let peer_channel = PeerChannel {
            message_tx: message_tx.clone(),
            closed: closed.clone(),
        };
        self.peer_channels.insert(channel_id, peer_channel);

        let frame = Frame {
            channel_id: uuid::Uuid::nil(),
            message: Message::Control(ControlMessage::OpenChannelResponse {
                channel_id,
                result: Ok(()),
            }),
        };
        if let Err(e) = self.framed.send(frame).await {
            tracing::error!("Failed to send open channel ack: {:?}", e);
            self.peer_channels.remove(&channel_id);
            return;
        }

        let (tx, rx) = channel(
            channel_id,
            channel_type,
            self.frame_tx.clone(),
            self.command_tx.clone(),
            message_rx,
            closed,
        );

        tokio::spawn(future_fn(tx, rx));
    }

    async fn handle_peer_ok(&mut self, channel_id: ChannelId) {
        tracing::info!("Received peer ack for opening channel [{}]", channel_id);
        match self.pending_open_channel_commands.remove(&channel_id) {
            Some(pending) => {
                // [7] Convert to "Peer Channel" oneshot sender.
                let closed = Arc::new(AtomicBool::new(false));
                let (message_tx, message_rx) = mpsc::channel(1);
                self.peer_channels.insert(
                    channel_id,
                    PeerChannel {
                        message_tx,
                        closed: closed.clone(),
                    },
                );
                let _ = pending
                    .reply
                    .send(Ok(OpenChannelCommandResult { message_rx, closed }));
            }
            None => {
                tracing::error!(
                    "Received open channel ack for unknown channel ID [{}]",
                    channel_id
                );
            }
        }
    }

    async fn handle_peer_err(&mut self, channel_id: ChannelId, err: String) {
        tracing::error!(
            "Received peer error for opening channel [{}]: {}",
            channel_id,
            err
        );
        match self.pending_open_channel_commands.remove(&channel_id) {
            Some(pending) => {
                let _ = pending
                    .reply
                    .send(Err(MuxError::ChannelOpenPeerFailed(err)));
            }
            None => {
                tracing::error!(
                    "Received open channel error for unknown channel ID [{}]",
                    channel_id
                );
            }
        }
    }

    async fn handle_peer_close(&mut self, channel_id: ChannelId) {
        tracing::info!("Received peer close for channel [{}]", channel_id);
        match self.peer_channels.remove(&channel_id) {
            Some(peer_channel) => {
                peer_channel
                    .closed
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                tracing::info!("Closed channel [{}]", channel_id);
            }
            None => {
                tracing::error!(
                    "Received close channel for unknown channel ID [{}]",
                    channel_id
                );
            }
        }
    }

    async fn dispatch_message(&mut self, channel_id: ChannelId, message: Message) {
        match self.peer_channels.get(&channel_id) {
            Some(channel) => {
                if let Err(e) = channel.message_tx.send(message).await {
                    tracing::error!(
                        "Failed to send message to channel [{}]: {:?}",
                        channel_id,
                        e
                    );
                }
            }
            None => {
                tracing::warn!("Received frame for unknown channel ID [{}]", channel_id);
            }
        }
    }

    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::OpenChannel {
                channel_id,
                channel_type,
                reply, // [5] Convert to "Peer Channel" oneshot sender.
            } => {
                if self.peer_channels.contains_key(&channel_id) {
                    let _ = reply.send(Err(MuxError::ChannelIdAlreadyExists(channel_id)));
                    return;
                }

                if self.pending_open_channel_commands.contains_key(&channel_id) {
                    let _ = reply.send(Err(MuxError::ChannelIdAlreadyExists(channel_id)));
                    return;
                }

                self.pending_open_channel_commands
                    .insert(channel_id, PendingOpenChannelCommand { reply });

                let frame = Frame {
                    channel_id: uuid::Uuid::nil(),
                    message: Message::Control(ControlMessage::OpenChannelRequest {
                        channel_id,
                        channel_type,
                    }),
                };
                if let Err(e) = self.framed.send(frame).await {
                    tracing::error!("Failed to send open channel request: {:?}", e);
                    if let Some(pending) = self.pending_open_channel_commands.remove(&channel_id) {
                        let _ = pending
                            .reply
                            .send(Err(MuxError::ChannelOpenRequestFailed(e.to_string())));
                    }
                }
            }
            Command::CloseChannel { channel_id } => {
                if self.peer_channels.remove(&channel_id).is_none() {
                    tracing::error!(
                        "Received request to close unknown channel ID [{}]",
                        channel_id
                    );
                    return;
                }

                let frame = Frame {
                    channel_id: uuid::Uuid::nil(),
                    message: Message::Control(ControlMessage::CloseChannel { channel_id }),
                };
                if let Err(e) = self.framed.send(frame).await {
                    tracing::error!("Failed to send close channel request: {:?}", e);
                }
                tracing::info!("Sent close channel request for channel [{}]", channel_id);
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
        tracing::info!("Mux dropped, closing {} channels", self.peer_channels.len());
    }
}
