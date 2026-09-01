use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use futures::SinkExt;
use futures::StreamExt;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::codec::FramedRead;
use tokio_util::codec::FramedWrite;
use tokio_util::sync::CancellationToken;

use crate::channel::ChannelAcceptor;
use crate::channel::ChannelId;
use crate::channel::ChannelReceiver;
use crate::channel::ChannelSender;
use crate::channel::ChannelType;
use crate::channel::channel;
use crate::codec::Codec;
use crate::codec::Frame;
use crate::message::ControlMessage;
use crate::message::Message;

#[derive(Debug, thiserror::Error)]
#[error("MuxError: {0}")]
pub struct MuxError(pub String);

#[derive(Debug)]
pub struct OpenChannelCommandReply {
    message_rx: mpsc::Receiver<Message>,
    channel_closed: CancellationToken,
}

#[derive(Debug)]
pub enum CloseChannelCommandReply {
    ChannelClosed(ChannelId),
    ChannelIdDoesNotExist(ChannelId),
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
        reply_tx: oneshot::Sender<Result<OpenChannelCommandReply, MuxError>>,
    },

    /// A command for the mux to close a channel.
    CloseChannel {
        channel_id: ChannelId,
        reply_tx: oneshot::Sender<CloseChannelCommandReply>,
    },
}

#[derive(Clone)]
pub struct MuxHandle {
    channel_outgoing_frame_tx: mpsc::Sender<Frame>,
    command_tx: mpsc::Sender<Command>,
}

impl MuxHandle {
    pub async fn open_channel(
        &self,
        channel_type: ChannelType,
    ) -> Result<(ChannelSender, ChannelReceiver, CancellationToken), MuxError> {
        tracing::info!("Requesting to open channel of type [{:?}]", channel_type);
        let channel_id = uuid::Uuid::now_v7();
        let (reply_tx, reply_rx) = oneshot::channel();
        self.command_tx
            .send(Command::OpenChannel {
                channel_id,
                channel_type,
                reply_tx,
            })
            .await
            .map_err(|e| MuxError(format!("Failed to send open channel command: {}", e)))?;

        let OpenChannelCommandReply {
            message_rx,
            channel_closed,
        } = reply_rx
            .await
            .map_err(|e| MuxError(format!("Open channel command resulted in an error: {}", e)))??;

        let (channel_tx, channel_rx) = channel(
            channel_id,
            channel_type,
            self.channel_outgoing_frame_tx.clone(),
            self.command_tx.clone(),
            message_rx,
        );

        Ok((channel_tx, channel_rx, channel_closed))
    }

    pub async fn close_channel(
        &self,
        channel_id: ChannelId,
    ) -> Result<CloseChannelCommandReply, MuxError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.command_tx
            .send(Command::CloseChannel {
                channel_id,
                reply_tx,
            })
            .await
            .map_err(|e| MuxError(format!("Failed to send close channel command: {}", e)))?;

        let reply = reply_rx
            .await
            .map_err(|e| MuxError(format!("Close channel command resulted in an error: {}", e)))?;

        Ok(reply)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MuxStatus {
    /// Mux task is running as normal.
    Running,

    /// Mux task is in the process of shutting down.
    ShuttingDown,
}

struct PendingChannel {
    channel_id: ChannelId,
    channel_type: ChannelType,
    reply_tx: oneshot::Sender<Result<OpenChannelCommandReply, MuxError>>,
}

impl std::fmt::Display for PendingChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Channel(channel_id={}, channel_type={})",
            self.channel_id, self.channel_type
        )
    }
}

#[derive(PartialEq, Eq)]
enum OpenChannelStatus {
    // The channel has not sent a close request and is not expecting a close response.
    NotAwaitingCloseResponse,

    // The channel has sent a close request and is expecting a close response.
    AwaitingCloseResponse,
}

struct OpenChannel {
    channel_id: ChannelId,
    channel_type: ChannelType,
    status: OpenChannelStatus,
    message_tx: mpsc::Sender<Message>,
    channel_closed: CancellationToken,
}

impl std::fmt::Display for OpenChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Channel(channel_id={}, channel_type={})",
            self.channel_id, self.channel_type
        )
    }
}

pub struct Mux<Acceptor>
where
    Acceptor: ChannelAcceptor,
{
    /// For receiving frames from the dedicated IO stream reader task.
    incoming_frame_rx: mpsc::Receiver<Frame>,

    /// For sending frames to the dedicated IO stream writer task.
    outgoing_frame_tx: mpsc::Sender<Frame>,

    /// For sending frames from channel handles.
    channel_outgoing_frame_tx: mpsc::Sender<Frame>,

    /// For forwarding frames from channel handles to the dedicated IO stream writer task.
    channel_outgoing_frame_rx: mpsc::Receiver<Frame>,

    command_tx: mpsc::Sender<Command>,

    command_rx: mpsc::Receiver<Command>,

    /// A handler for incoming channel open requests from the peer.
    channel_acceptor: Acceptor,

    /// A cancellation token that, when cancelled, will close all channels and terminate the mux.
    cancellation_token: CancellationToken,

    status: MuxStatus,

    /// A dispatch table mapping channel IDs to message senders.
    open_channels: HashMap<ChannelId, OpenChannel>,

    /// A mapping of pending channels that have not yet been acknowledged by the peer.
    pending_channels: HashMap<ChannelId, PendingChannel>,
}

impl<Acceptor> Mux<Acceptor>
where
    Acceptor: ChannelAcceptor,
{
    pub fn spawn<R, W>(
        reader: R,
        writer: W,
        channel_acceptor: Acceptor,
        cancellation_token: CancellationToken,
    ) -> (JoinHandle<()>, MuxHandle)
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        let incoming_frame_rx = {
            // TODO: Tweak channel capacity for performance.
            let (incoming_frame_tx, incoming_frame_rx) = mpsc::channel(1);

            tokio::spawn(async move {
                let mut framed_reader = FramedRead::new(reader, Codec::new());

                loop {
                    tokio::select! {
                        frame_result_opt = framed_reader.next() => {
                            match frame_result_opt {
                                None => {
                                    tracing::info!("Peer connection has dropped.");
                                    break;
                                }
                                Some(Err(e)) => {
                                    tracing::info!("Failed to read frame from peer connection: {}", e);
                                    break;
                                }
                                Some(Ok(frame)) => {
                                    if let Err(e) = incoming_frame_tx.send(frame).await {
                                        tracing::error!("Failed to send frame: {}", e);
                                        break;
                                    }
                                }
                            }
                        }
                        _ = incoming_frame_tx.closed() => {
                            tracing::info!("Mux actor frame receiver has dropped.");
                            break;
                        }
                    }
                }

                tracing::info!("Mux incoming frame reader task exiting.");
            });

            incoming_frame_rx
        };

        let outgoing_frame_tx = {
            // TODO: Tweak channel capacity for performance.
            let (outgoing_frame_tx, mut outgoing_frame_rx) = mpsc::channel(1);

            tokio::spawn(async move {
                let mut framed_writer = FramedWrite::new(writer, Codec::new());

                loop {
                    match outgoing_frame_rx.recv().await {
                        Some(frame) => {
                            if let Err(e) = framed_writer.send(frame).await {
                                tracing::info!(
                                    "Dropping outgoing frame because IO stream has closed: {}",
                                    e
                                );
                            }
                        }
                        None => {
                            tracing::info!("Mux actor frame sender has dropped.");
                            break;
                        }
                    }
                }

                tracing::info!("Mux outgoing frame writer task exiting.");
            });

            outgoing_frame_tx
        };

        let (command_tx, command_rx) = mpsc::channel(1);

        let mux_command_tx = command_tx.clone();
        let (channel_outgoing_frame_tx, channel_outgoing_frame_rx) = mpsc::channel(1);
        let mux_channel_outgoing_frame_tx = channel_outgoing_frame_tx.clone();
        let join_handle = tokio::spawn(async move {
            Self {
                incoming_frame_rx,
                outgoing_frame_tx,
                channel_outgoing_frame_tx: mux_channel_outgoing_frame_tx,
                channel_outgoing_frame_rx,
                command_rx,
                channel_acceptor,
                cancellation_token,
                command_tx: mux_command_tx,
                status: MuxStatus::Running,
                open_channels: HashMap::new(),
                pending_channels: HashMap::new(),
            }
            .run()
            .await
        });

        let mux_handle = MuxHandle {
            channel_outgoing_frame_tx,
            command_tx,
        };

        (join_handle, mux_handle)
    }

    async fn run(&mut self) {
        loop {
            if self.status == MuxStatus::ShuttingDown && self.open_channels.is_empty() {
                break;
            }

            tokio::select! {
                _ = self.cancellation_token.cancelled(), if self.status == MuxStatus::Running => {
                    self.shutdown();
                }
                frame_opt = self.incoming_frame_rx.recv() => {
                    match frame_opt {
                        Some(frame) => {
                            self.recv_incoming_frame(frame).await;
                        }
                        None => {
                            tracing::info!("Incoming frame task has dropped.");
                            break;
                        }
                    }
                }
                frame_opt = self.channel_outgoing_frame_rx.recv() => {
                    match frame_opt {
                        Some(frame) => {
                            self.send_outgoing_frame(frame).await;
                        }
                        None => {
                            tracing::info!("Channel outgoing frame task has dropped.");
                            break;
                        }
                    }
                }
                command_opt = self.command_rx.recv() => {
                    match command_opt {
                        Some(command) => {
                            self.handle_command(command).await;
                        }
                        None => {
                            tracing::info!("All mux handles dropped.");
                            break;
                        }
                    }
                }
            }
        }

        tracing::info!("Mux actor task is terminating.");
    }

    async fn send_outgoing_frame(&mut self, frame: Frame) {
        if frame.channel_id.is_nil() {
            if let Err(e) = self.outgoing_frame_tx.send(frame).await {
                tracing::error!("Failed to send outgoing frame: {}", e)
            }
            return;
        }

        match self.open_channels.get(&frame.channel_id) {
            Some(OpenChannel {
                status: OpenChannelStatus::NotAwaitingCloseResponse,
                ..
            }) => {
                if let Err(e) = self.outgoing_frame_tx.send(frame).await {
                    tracing::error!("Failed to send outgoing frame: {}", e)
                }
            }
            Some(OpenChannel {
                status: OpenChannelStatus::AwaitingCloseResponse,
                ..
            }) => {
                tracing::error!(
                    "Attempted to send channel frame for channel that is awaiting close response: {}",
                    frame.channel_id
                )
            }
            None => {
                tracing::error!(
                    "Attempted to send channel frame for unknown channel: {}",
                    frame.channel_id
                )
            }
        }
    }

    /// Drain all pending channels and close all channels.
    fn shutdown(&mut self) {
        tracing::info!("Shutting down mux...");
        self.status = MuxStatus::ShuttingDown;

        tracing::info!(
            "Canceling {} pending channels...",
            self.pending_channels.len()
        );
        for (_, pending_channel) in self.pending_channels.drain() {
            let _ = pending_channel.reply_tx.send(Err(MuxError(
                "Cannot open new channel. Mux is shutting down".to_string(),
            )));
        }

        // Enqueue close channel commands for all open channels.
        // Mux::shutdown is synchronous so we cannot enqueue close channel commands directly.
        // Instead, we spawn a separate task so the main mux task can return to the Mux::run loop.
        let open_channel_ids = self.open_channels.keys().copied().collect::<Vec<_>>();
        let command_tx = self.command_tx.clone();

        tokio::spawn(async move {
            for channel_id in open_channel_ids {
                let (reply_tx, _reply_rx) = oneshot::channel();
                if let Err(e) = command_tx
                    .send(Command::CloseChannel {
                        channel_id,
                        reply_tx,
                    })
                    .await
                {
                    tracing::error!(
                        "Failed to send close channel command during shutdown: {}",
                        e
                    )
                }
            }
        });

        tracing::info!("Awaiting shutdown completion by peer mux...");
    }

    /// Handle an incoming frame from the peer.
    async fn recv_incoming_frame(&mut self, frame: Frame) {
        if frame.channel_id.is_nil() {
            // Message for control channel.
            match frame.message {
                Message::Control(ControlMessage::OpenChannelRequest {
                    channel_id,
                    channel_type,
                }) => {
                    self.handle_open_channel_request(channel_id, channel_type)
                        .await;
                }
                Message::Control(ControlMessage::OpenChannelResponse { channel_id, result }) => {
                    self.handle_open_channel_response(channel_id, result).await;
                }
                Message::Control(ControlMessage::CloseChannelRequest { channel_id }) => {
                    tracing::info!("Received close channel request for ID [{}]", channel_id);
                    self.handle_close_channel_request(channel_id).await;
                }
                Message::Control(ControlMessage::CloseChannelResponse { channel_id, result }) => {
                    tracing::info!(
                        "Received close channel response for ID [{}]: {:?}",
                        channel_id,
                        result
                    );
                    self.handle_close_channel_response(channel_id, result).await;
                }
                _ => {
                    tracing::error!("Unexpected message on control channel: {:?}", frame.message);
                    tracing::warn!("Shutting down mux.");
                    self.shutdown();
                }
            }
        } else {
            // Message for data channel.
            self.dispatch_message(frame.channel_id, frame.message).await;
        }
    }

    /// Handling a request from the peer about opening a channel.
    async fn handle_open_channel_request(
        &mut self,
        channel_id: ChannelId,
        channel_type: ChannelType,
    ) {
        if let Some(channel) = self.open_channels.get(&channel_id) {
            tracing::error!(
                "Received open channel request for channel ID that is already open: [{}]",
                channel,
            );

            self.send_outgoing_frame(Frame {
                channel_id: ChannelId::nil(),
                message: Message::Control(ControlMessage::OpenChannelResponse {
                    channel_id,
                    result: Err(format!("Channel ID [{}] is already open", channel_id)),
                }),
            })
            .await;

            return;
        }

        if let Some(channel) = self.pending_channels.get(&channel_id) {
            tracing::error!(
                "Received open channel request for channel ID that is already pending: [{}]",
                channel,
            );

            self.send_outgoing_frame(Frame {
                channel_id: ChannelId::nil(),
                message: Message::Control(ControlMessage::OpenChannelResponse {
                    channel_id,
                    result: Err(format!("Channel ID [{}] is already pending", channel_id)),
                }),
            })
            .await;

            return;
        }

        if self.status == MuxStatus::ShuttingDown {
            tracing::warn!("Received open channel request while shutting down",);

            self.send_outgoing_frame(Frame {
                channel_id: ChannelId::nil(),
                message: Message::Control(ControlMessage::OpenChannelResponse {
                    channel_id,
                    result: Err("Peer mux is shutting down".to_string()),
                }),
            })
            .await;

            return;
        }

        let future_fn = match self.channel_acceptor.future_fn(channel_id, channel_type) {
            Ok(future_fn) => future_fn,
            Err(err) => {
                tracing::error!("Failed to accept open channel request: {}", err);

                self.send_outgoing_frame(Frame {
                    channel_id: ChannelId::nil(),
                    message: Message::Control(ControlMessage::OpenChannelResponse {
                        channel_id,
                        result: Err(err),
                    }),
                })
                .await;

                return;
            }
        };

        // TODO: Tweak channel capacity for performance.
        let (message_tx, message_rx) = mpsc::channel(1);
        let channel_closed = CancellationToken::new();

        let open_channel = OpenChannel {
            channel_id: channel_id.clone(),
            channel_type: channel_type.clone(),
            status: OpenChannelStatus::NotAwaitingCloseResponse,
            message_tx,
            channel_closed: channel_closed.clone(),
        };

        self.open_channels.insert(channel_id, open_channel);

        self.send_outgoing_frame(Frame {
            channel_id: ChannelId::nil(),
            message: Message::Control(ControlMessage::OpenChannelResponse {
                channel_id,
                result: Ok(()),
            }),
        })
        .await;

        let (tx, rx) = channel(
            channel_id,
            channel_type,
            self.channel_outgoing_frame_tx.clone(),
            self.command_tx.clone(),
            message_rx,
        );

        tokio::spawn(future_fn(tx, rx, channel_closed));
    }

    /// Handle a response from the peer about opening a channel.
    async fn handle_open_channel_response(
        &mut self,
        channel_id: ChannelId,
        result: Result<(), String>,
    ) {
        let pending_channel = match self.pending_channels.remove(&channel_id) {
            Some(channel) => channel,
            None => {
                tracing::warn!(
                    "Received open channel response for non-pending channel ID [{}]",
                    channel_id
                );
                return;
            }
        };

        match result {
            Ok(()) => {
                // TODO: Tweak channel capacity for performance.
                let (message_tx, message_rx) = mpsc::channel(1);
                let channel_closed = CancellationToken::new();

                self.open_channels.insert(
                    channel_id,
                    OpenChannel {
                        channel_id,
                        channel_type: pending_channel.channel_type,
                        status: OpenChannelStatus::NotAwaitingCloseResponse,
                        message_tx,
                        channel_closed: channel_closed.clone(),
                    },
                );

                let _ = pending_channel.reply_tx.send(Ok(OpenChannelCommandReply {
                    message_rx,
                    channel_closed,
                }));
            }
            Err(err) => {
                tracing::error!("Failed to open channel [{}]: {}", pending_channel, err);
            }
        }
    }

    /// Handle a request from the peer about closing a channel.
    async fn handle_close_channel_request(&mut self, channel_id: ChannelId) {
        match self.open_channels.remove(&channel_id) {
            Some(open_channel) => {
                match open_channel.status {
                    OpenChannelStatus::NotAwaitingCloseResponse => {
                        // Allow open_channel to be removed and notify waiters.
                        open_channel.channel_closed.cancel();
                    }
                    OpenChannelStatus::AwaitingCloseResponse => {
                        // Restore open_channel and wait for close response.
                        self.open_channels.insert(channel_id, open_channel);
                    }
                }

                self.send_outgoing_frame(Frame {
                    channel_id: ChannelId::nil(),
                    message: Message::Control(ControlMessage::CloseChannelResponse {
                        channel_id,
                        result: Ok(()),
                    }),
                })
                .await;
            }
            None => {
                tracing::warn!(
                    "Received close channel request for non-open channel ID [{}]",
                    channel_id
                );

                self.send_outgoing_frame(Frame {
                    channel_id: ChannelId::nil(),
                    message: Message::Control(ControlMessage::CloseChannelResponse {
                        channel_id,
                        result: Err(format!("Channel ID [{}] not open", channel_id)),
                    }),
                })
                .await;
            }
        }
    }

    /// Handle a response from the peer about closing a channel.
    async fn handle_close_channel_response(
        &mut self,
        channel_id: ChannelId,
        result: Result<(), String>,
    ) {
        if let Err(e) = result {
            tracing::warn!("Peer failed to close channel [{}]: {}", channel_id, e);
        }

        match self.open_channels.remove(&channel_id) {
            Some(open_channel) => {
                match open_channel.status {
                    OpenChannelStatus::NotAwaitingCloseResponse => {
                        // Protocol failure. This side did not send close request so remote should not send response.
                        // Restore open_channel and keep it open.
                        tracing::warn!(
                            "Received close channel response for not-awaiting-close channel ID [{}]",
                            channel_id
                        );
                    }
                    OpenChannelStatus::AwaitingCloseResponse => {
                        // Allow open_channel to drop and notify waiters.
                        open_channel.channel_closed.cancel();
                    }
                };
            }
            None => {
                tracing::warn!(
                    "Received close channel response for non-open channel ID [{}]",
                    channel_id
                );
            }
        }
    }

    /// Dispatch an incoming message to a local channel.
    async fn dispatch_message(&mut self, channel_id: ChannelId, message: Message) {
        match self.open_channels.get(&channel_id) {
            Some(open_channel) => match open_channel.status {
                OpenChannelStatus::NotAwaitingCloseResponse => {
                    if let Err(e) = open_channel.message_tx.send(message).await {
                        tracing::warn!("Failed to send message on channel [{}]: {}", channel_id, e);
                    }
                }
                OpenChannelStatus::AwaitingCloseResponse => {
                    tracing::warn!(
                        "Incoming message for awaiting-close channel ID [{}]. Dropping message.",
                        channel_id
                    );
                }
            },
            None => {
                tracing::warn!(
                    "Received message for for non-opened channel [{}]: {:?}",
                    channel_id,
                    message
                );
            }
        }
    }

    /// Handle a command from a local task.
    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::OpenChannel {
                channel_id,
                channel_type,
                reply_tx,
            } => {
                self.handle_open_channel_command(channel_id, channel_type, reply_tx)
                    .await;
            }
            Command::CloseChannel {
                channel_id,
                reply_tx,
            } => {
                self.handle_close_channel_command(channel_id, reply_tx)
                    .await;
            }
        }
    }

    /// Handle a command from a local task to open a new channel.
    async fn handle_open_channel_command(
        &mut self,
        channel_id: ChannelId,
        channel_type: ChannelType,
        reply_tx: oneshot::Sender<Result<OpenChannelCommandReply, MuxError>>,
    ) {
        if self.open_channels.contains_key(&channel_id) {
            let _ = reply_tx.send(Err(MuxError(format!(
                "Channel ID [{}] already open",
                channel_id
            ))));
            return;
        }

        if self.pending_channels.contains_key(&channel_id) {
            let _ = reply_tx.send(Err(MuxError(format!(
                "Channel ID [{}] already pending",
                channel_id
            ))));
            return;
        }

        if self.status == MuxStatus::ShuttingDown {
            let _ = reply_tx.send(Err(MuxError(format!(
                "Channel ID [{}] cannot be opened. Mux is shutting down.",
                channel_id
            ))));
            return;
        }

        self.pending_channels.insert(
            channel_id,
            PendingChannel {
                channel_id,
                channel_type,
                reply_tx,
            },
        );

        self.send_outgoing_frame(Frame {
            channel_id: ChannelId::nil(),
            message: Message::Control(ControlMessage::OpenChannelRequest {
                channel_id,
                channel_type,
            }),
        })
        .await;
    }

    /// Handle a command from a local task to close a channel.
    async fn handle_close_channel_command(
        &mut self,
        channel_id: ChannelId,
        reply_tx: oneshot::Sender<CloseChannelCommandReply>,
    ) {
        match self.open_channels.get_mut(&channel_id) {
            Some(open_channel) => {
                let _ = reply_tx.send(CloseChannelCommandReply::ChannelClosed(channel_id));

                if open_channel.status == OpenChannelStatus::AwaitingCloseResponse {
                    return;
                }

                open_channel.status = OpenChannelStatus::AwaitingCloseResponse;

                self.send_outgoing_frame(Frame {
                    channel_id: ChannelId::nil(),
                    message: Message::Control(ControlMessage::CloseChannelRequest { channel_id }),
                })
                .await;
            }
            None => {
                let _ = reply_tx.send(CloseChannelCommandReply::ChannelIdDoesNotExist(channel_id));
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use crate::channel::FnChannelAcceptor;

    use super::*;

    fn noop_acceptor() -> impl ChannelAcceptor {
        FnChannelAcceptor::new(|_id, _ty| Ok(Box::new(|_tx, _rx, _notify| Box::pin(async {}))))
    }

    struct TestMuxPair {
        mux_handle_a: MuxHandle,
        mux_handle_b: MuxHandle,
        cancellation_token_a: CancellationToken,
        cancellation_token_b: CancellationToken,
        join_handle_a: JoinHandle<()>,
        join_handle_b: JoinHandle<()>,
    }

    impl TestMuxPair {
        fn new() -> Self {
            let (a_io, b_io) = tokio::io::duplex(64 * 1024);

            let (a_reader, a_writer) = tokio::io::split(a_io);
            let (b_reader, b_writer) = tokio::io::split(b_io);

            let cancellation_token_a = CancellationToken::new();
            let cancellation_token_b = CancellationToken::new();

            let (join_handle_a, mux_handle_a) = Mux::spawn(
                a_reader,
                a_writer,
                noop_acceptor(),
                cancellation_token_a.clone(),
            );

            let (join_handle_b, mux_handle_b) = Mux::spawn(
                b_reader,
                b_writer,
                noop_acceptor(),
                cancellation_token_b.clone(),
            );

            Self {
                mux_handle_a,
                mux_handle_b,
                cancellation_token_a,
                cancellation_token_b,
                join_handle_a,
                join_handle_b,
            }
        }
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn cancel_mux_with_one_open_channel() {
        // Setup
        let TestMuxPair {
            mux_handle_a,
            mux_handle_b: _mux_handle_b,
            cancellation_token_a,
            cancellation_token_b: _cancellation_token_b,
            join_handle_a,
            join_handle_b,
        } = TestMuxPair::new();

        // Simulate task that drops initial mux_handle;
        {
            let mux_handle_a = mux_handle_a;
            tokio::spawn(async move {
                let (channel_sender_a, channel_receiver_a, _notify) = mux_handle_a
                    .open_channel(ChannelType::Worker)
                    .await
                    .expect("channel should open");

                tokio::spawn(async move {
                    let _ = (channel_sender_a, channel_receiver_a, _notify);
                })
            });
        }

        // Simulate task that cancels mux A.
        cancellation_token_a.cancel();

        // Assert mux task A terminates.
        tokio::time::timeout(Duration::from_secs(1), join_handle_a)
            .await
            .expect("mux task A did not terminate before timeout")
            .expect("mux task A panicked");

        // Assert mux task B terminates.
        tokio::time::timeout(Duration::from_secs(1), join_handle_b)
            .await
            .expect("mux task B did not terminate before timeout")
            .expect("mux task B panicked");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn cancel_mux_with_ten_open_channels() {
        // Setup
        let TestMuxPair {
            mux_handle_a,
            mux_handle_b: _mux_handle_b,
            cancellation_token_a,
            cancellation_token_b: _cancellation_token_b,
            join_handle_a,
            join_handle_b,
        } = TestMuxPair::new();

        // Simulate task that drops initial mux_handle;
        {
            let mux_handle_a = mux_handle_a;
            for _ in 0..10 {
                let mux_handle_a = mux_handle_a.clone();
                tokio::spawn(async move {
                    let (channel_sender_a, channel_receiver_a, _notify) = mux_handle_a
                        .open_channel(ChannelType::Worker)
                        .await
                        .expect("channel should open");

                    tokio::spawn(async move {
                        let _ = (channel_sender_a, channel_receiver_a, _notify);
                    });
                });
            }
        }

        // Simulate task that cancels mux A.
        cancellation_token_a.cancel();

        // Assert that mux task A terminates.
        tokio::time::timeout(Duration::from_secs(1), join_handle_a)
            .await
            .expect("mux task A did not terminate before timeout")
            .expect("mux task A panicked");

        // Assert that mux task B terminates.
        tokio::time::timeout(Duration::from_secs(1), join_handle_b)
            .await
            .expect("mux task B did not terminate before timeout")
            .expect("mux task B panicked");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn cancel_mux_with_pending_channel() {
        // Setup cancellation token and IO stream.
        let mux_cancellation_token = CancellationToken::new();
        let (mux_io, peer_io) = tokio::io::duplex(4096);

        // Spawn mux task.
        let (mux_join_handle, mux_handle) = {
            let (mux_reader, mux_writer) = tokio::io::split(mux_io);
            Mux::spawn(
                mux_reader,
                mux_writer,
                noop_acceptor(),
                mux_cancellation_token.clone(),
            )
        };

        // Simulate task that drops initial mux_handle.
        let reply_rx = {
            let mux_handle = mux_handle;
            let (reply_tx, reply_rx) = oneshot::channel();
            mux_handle
                .command_tx
                .send(Command::OpenChannel {
                    channel_id: ChannelId::now_v7(),
                    channel_type: ChannelType::Worker,
                    reply_tx,
                })
                .await
                .unwrap();

            reply_rx
        };

        // Peer reads open channel request, but does not send response.
        let mut peer_frame_reader = {
            let (peer_reader, _peer_writer) = tokio::io::split(peer_io);
            FramedRead::new(peer_reader, Codec::new())
        };

        let open_channel_request_frame =
            tokio::time::timeout(Duration::from_secs(1), peer_frame_reader.next())
                .await
                .expect("peer frame reader did not receive anything before timeout")
                .expect("peer frame reader received None")
                .expect("peer frame reader received a codec error");
        assert!(matches!(
            open_channel_request_frame,
            Frame {
                message: Message::Control(ControlMessage::OpenChannelRequest {
                    channel_type: ChannelType::Worker,
                    ..
                }),
                ..
            }
        ));

        // Cancel mux before peer sends response.
        mux_cancellation_token.cancel();

        // Assert that the open channel command replied with an error.
        let result = tokio::time::timeout(Duration::from_secs(1), reply_rx)
            .await
            .expect("open channel command did not reply before timeout")
            .expect("open channel command was dropped");
        assert!(matches!(result, Err(MuxError(_))));

        // Assert that the mux task terminates.
        tokio::time::timeout(Duration::from_secs(1), mux_join_handle)
            .await
            .expect("mux task did not terminate before timeout")
            .expect("mux task panicked");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[tracing_test::traced_test]
    async fn mux_and_peer_cancel_concurrently() {
        // Setup
        let TestMuxPair {
            mux_handle_a,
            mux_handle_b,
            cancellation_token_a,
            cancellation_token_b,
            join_handle_a,
            join_handle_b,
        } = TestMuxPair::new();

        drop(mux_handle_a);
        drop(mux_handle_b);

        let barrier = Arc::new(tokio::sync::Barrier::new(2));

        let barrier_a = barrier.clone();
        let cancel_a_join_handle = tokio::spawn(async move {
            barrier_a.wait().await;
            cancellation_token_a.cancel();
        });

        let barrier_b = barrier;
        let cancel_b_join_handle = tokio::spawn(async move {
            barrier_b.wait().await;
            cancellation_token_b.cancel();
        });

        cancel_a_join_handle
            .await
            .expect("cancel mux A task panicked");
        cancel_b_join_handle
            .await
            .expect("cancel mux B task panicked");

        tokio::time::timeout(Duration::from_secs(1), join_handle_a)
            .await
            .expect("mux task A did not exit before the timeout")
            .expect("mux task A panicked");

        tokio::time::timeout(Duration::from_secs(1), join_handle_b)
            .await
            .expect("mux task B did not exit before the timeout")
            .expect("mux task B panicked");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn mux_opens_channel_when_io_stream_is_dropped() {
        // Setup IO stream.
        let (mux_io, peer_io) = tokio::io::duplex(4096);

        drop(peer_io);

        // Mux sends open channel request.
        let (mux_reader, mux_writer) = tokio::io::split(mux_io);
        let (mux_join_handle, mux_handle) = Mux::spawn(
            mux_reader,
            mux_writer,
            noop_acceptor(),
            CancellationToken::new(),
        );

        let result = mux_handle.open_channel(ChannelType::Worker).await;

        assert!(matches!(result, Err(MuxError(_))));

        tokio::time::timeout(Duration::from_secs(1), mux_join_handle)
            .await
            .expect("mux task did not exit before timeout")
            .expect("mux task panicked");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn open_channel_fails_when_peer_is_shutting_down() {
        // Setup IO stream.
        let (mux_io, peer_io) = tokio::io::duplex(4096);

        let (mux_reader, mux_writer) = tokio::io::split(mux_io);
        let (mux_join_handle, mux_handle) = Mux::spawn(
            mux_reader,
            mux_writer,
            noop_acceptor(),
            CancellationToken::new(),
        );

        let (peer_reader, peer_writer) = tokio::io::split(peer_io);
        let peer_cancellation_token = CancellationToken::new();
        let (peer_join_handle, peer_handle) = Mux::spawn(
            peer_reader,
            peer_writer,
            noop_acceptor(),
            peer_cancellation_token.clone(),
        );

        // Simulate task that drops initial peer mux_handle.
        drop(peer_handle);

        let (_mux_channel_sender, _mux_channel_receiver, _notify) = mux_handle
            .open_channel(ChannelType::Worker)
            .await
            .expect("failed to open channel");

        // Simulate task that cancels peer mux to initiate shutdown.
        peer_cancellation_token.cancel();

        // Allow peer to observe cancellation. Existing channel should cause peer shutdown to await.
        tokio::task::yield_now().await;

        // Assert that mux open channel returns error because peer is shutting down.
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            mux_handle.open_channel(ChannelType::Worker),
        )
        .await
        .expect("mux open channel did not return before timeout");
        assert!(matches!(result, Err(MuxError(_)),));

        // Assert that mux task terminates.
        tokio::time::timeout(Duration::from_secs(1), mux_join_handle)
            .await
            .expect("mux task did not exit before timeout")
            .expect("mux task panicked");

        // Assert that peer mux task terminates.
        tokio::time::timeout(Duration::from_secs(1), peer_join_handle)
            .await
            .expect("peer mux task did not exit before timeout")
            .expect("peer mux task panicked");
    }
}
