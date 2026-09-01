//! A channel is a bidirectional data stream between two peers.
//! A channel does not support broadcasting. A channel handle should only
//! have a single owner.
//!
//! In some cases, it may be helpful to split the ownership of a channel handle
//! into a sender and a receiver.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use futures::Stream;
use futures::future::BoxFuture;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncWrite;
use tokio::io::{self};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::StreamReader;
use tokio_util::sync::CancellationToken;
use tokio_util::sync::PollSender;

use crate::codec::Frame;
use crate::message::Message;
use crate::mux::Command;
use crate::mux::MuxError;

pub type ChannelId = uuid::Uuid;

#[derive(
    Debug,
    Clone,
    Copy,
    Serialize,
    Deserialize,
    strum::EnumString,
    strum::Display
)]
pub enum ChannelType {
    /// An evaluator scheduler channel. Initiated by a runner.
    EvaluatorScheduler,

    /// An evaluator channel. Initiated by a runner.
    Evaluator,

    /// A worker scheduler channel. Initiated by a runner.
    WorkerScheduler,

    /// A worker channel. Initiated by a runner.
    Worker,

    /// A debugger scheduler channel. Initiated by a runner.
    DebuggerScheduler,

    /// A debugger channel. Initiated by a runner.
    Debugger,

    /// A debug resolver channel. Initiated by a CLI.
    DebugResolver,

    /// A debug client channel. Initiated by a CLI.
    DebugClient,

    /// A debug client tunnel channel. Initiated by a CLI.
    DebugClientTunnel,

    /// A debugger tunnel channel. Initiated by a runner.
    DebuggerTunnel,
}

pub fn channel(
    channel_id: ChannelId,
    channel_type: ChannelType,
    frame_tx: mpsc::Sender<Frame>,
    command_tx: mpsc::Sender<Command>,
    message_rx: mpsc::Receiver<Message>,
) -> (ChannelSender, ChannelReceiver) {
    let sender = ChannelSender {
        channel_id,
        channel_type,
        frame_tx,
        command_tx: command_tx.clone(),
    };

    let receiver = ChannelReceiver {
        channel_id,
        channel_type,
        message_stream: ReceiverStream::new(message_rx),
        command_tx: command_tx.clone(),
    };

    (sender, receiver)
}

/// A handle to a channel that can be used to send messages to the peer and receive messages from the peer.
/// The channel handle is owned by a single task.
/// In some cases, it may be useful to split the ownership of a channel handle into a sender and a receiver.
/// This can be done using [`ChannelHandle::take_message_rx`] to take the message receiver out of the channel handle and give it to another task.
#[derive(Debug, Clone)]
pub struct ChannelSender {
    /// The channel's unique identifier.
    channel_id: ChannelId,

    channel_type: ChannelType,

    /// A sender for outbound frames, from a local task to the peer task.
    frame_tx: mpsc::Sender<Frame>,

    /// A sender for commands to the mux task, such as closing the channel.
    command_tx: mpsc::Sender<Command>,
}

impl ChannelSender {
    /// Sends a [`Message`] to the peer.
    pub async fn send(&self, message: Message) -> Result<(), MuxError> {
        let frame = Frame {
            channel_id: self.channel_id,
            message,
        };

        self.frame_tx.send(frame).await.map_err(|e| {
            MuxError(format!(
                "ChannelSender [{}][{}] failed to send frame: {}",
                self.channel_type, self.channel_id, e
            ))
        })
    }
}

pub struct ChannelPollSender {
    /// The channel's unique identifier.
    channel_id: ChannelId,

    channel_type: ChannelType,

    /// A sender for outbound frames, from a local task to the peer task.
    frame_tx: PollSender<Frame>,

    /// A sender for commands to the mux task, such as closing the channel.
    command_tx: mpsc::Sender<Command>,
}

impl ChannelPollSender {
    pub fn new(sender: ChannelSender) -> Self {
        Self {
            channel_id: sender.channel_id,
            channel_type: sender.channel_type,
            frame_tx: PollSender::new(sender.frame_tx),
            command_tx: sender.command_tx,
        }
    }

    /// Poll-based (synchronous) reservation of a permit to send a message.
    pub fn poll_reserve(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), MuxError>> {
        match self.frame_tx.poll_reserve(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(MuxError(format!(
                "ChannelSender [{}][{}] failed to poll reserve: {}",
                self.channel_type, self.channel_id, e
            )))),
            Poll::Pending => Poll::Pending,
        }
    }

    pub fn send_item(&mut self, value: Frame) -> Result<(), MuxError> {
        self.frame_tx.send_item(value).map_err(|e| {
            MuxError(format!(
                "ChannelSender [{}][{}] failed to send item: {}",
                self.channel_type, self.channel_id, e
            ))
        })
    }
}

const MAX_FRAME_SIZE: usize = 1024 * 1024;

impl AsyncWrite for ChannelPollSender {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let n = buf.len().min(MAX_FRAME_SIZE);
        let channel_id = self.channel_id.clone();

        match self.poll_reserve(cx) {
            Poll::Ready(Ok(())) => {
                let frame = Frame {
                    channel_id,
                    message: Message::RawData(buf[..n].to_vec()),
                };

                match self.send_item(frame) {
                    Ok(_) => Poll::Ready(Ok(n)),
                    Err(e) => Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e))),
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // mpsc has no internal buffering to flush beyond the channel itself.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Half-close: tell the mux task this side is done writing.
        let (reply_tx, _reply_rx) = oneshot::channel();
        let _ = self.command_tx.send(Command::CloseChannel {
            channel_id: self.channel_id,
            reply_tx,
        });
        Poll::Ready(Ok(()))
    }
}

pub struct ChannelReceiver {
    /// The channel's unique identifier.
    channel_id: ChannelId,

    channel_type: ChannelType,

    /// A receiver for inbound messages from the peer task.
    message_stream: ReceiverStream<Message>,

    /// A sender for commands to the mux task, such as closing the channel.
    command_tx: mpsc::Sender<Command>,
}

impl ChannelReceiver {
    /// Receives a [`Message`] from the channel.
    /// Returns [`None`] if the channel has been closed.
    pub async fn recv(&mut self) -> Option<Message> {
        self.message_stream.next().await
    }
}

impl Stream for ChannelReceiver {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.message_stream).poll_next(cx) {
            Poll::Ready(Some(message)) => match message {
                Message::RawData(data) => Poll::Ready(Some(Ok(Bytes::from(data)))),
                _ => Poll::Ready(None),
            },
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for ChannelReceiver {
    fn drop(&mut self) {
        tracing::info!(
            "Dropping ChannelReceiver [{:?}][{}]. Spawning task to send close command.",
            self.channel_type,
            self.channel_id
        );

        let command_tx = self.command_tx.clone();
        let channel_id = self.channel_id.clone();

        tokio::spawn(async move {
            let (reply_tx, _reply_rx) = oneshot::channel();

            if let Err(_) = command_tx
                .send(Command::CloseChannel {
                    channel_id,
                    reply_tx,
                })
                .await
            {
                tracing::info!(
                    "Unable to send close command for channel_id [{}] because mux task has already terminated.",
                    channel_id,
                );
            }
        });
    }
}

pub type ChannelByteStream =
    tokio::io::Join<StreamReader<ChannelReceiver, Bytes>, ChannelPollSender>;

pub fn combine_into_byte_stream(
    sender: ChannelSender,
    receiver: ChannelReceiver,
) -> ChannelByteStream {
    let async_read = StreamReader::new(receiver);
    let async_write = ChannelPollSender::new(sender);
    tokio::io::join(async_read, async_write)
}

/// A function that accepts a channel handle and returns a future that sends
/// and receives messages on the channel.
pub type ChannelFutureFn =
    dyn FnOnce(ChannelSender, ChannelReceiver, CancellationToken) -> BoxFuture<'static, ()> + Send;

/// Provides an operation to handle a channel open request from the peer.
pub trait ChannelAcceptor
where
    Self: Clone + Send + 'static,
{
    fn future_fn(
        &self,
        channel_id: ChannelId,
        channel_type: ChannelType,
    ) -> Result<Box<ChannelFutureFn>, String>;
}

/// A [`ChannelAcceptor`] that is constructed from a closure.
#[derive(Clone)]
pub struct FnChannelAcceptor<F> {
    f: F,
}

impl<F> FnChannelAcceptor<F>
where
    F: Fn(ChannelId, ChannelType) -> Result<Box<ChannelFutureFn>, String> + Clone + Send + 'static,
{
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

impl<F> ChannelAcceptor for FnChannelAcceptor<F>
where
    F: Fn(ChannelId, ChannelType) -> Result<Box<ChannelFutureFn>, String> + Clone + Send + 'static,
{
    fn future_fn(
        &self,
        channel_id: ChannelId,
        channel_type: ChannelType,
    ) -> Result<Box<ChannelFutureFn>, String> {
        (self.f)(channel_id, channel_type)
    }
}

/// Convenience function: converts an async fn(ChannelHandle) into the
/// boxed FnOnce that ChannelAcceptor::accept must return.
///
/// Use this inside your FnChannelAcceptor closure to avoid writing
/// Box::new and Box::pin at every call site.
pub fn accept<F, Fut>(f: F) -> Box<ChannelFutureFn>
where
    F: FnOnce(ChannelSender, ChannelReceiver, CancellationToken) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Box::new(move |tx, rx, notify| Box::pin(f(tx, rx, notify)))
}
