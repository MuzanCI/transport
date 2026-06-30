//! A channel is a bidirectional data stream between two peers.
//! A channel does not support broadcasting. A channel handle should only
//! have a single owner.
//!
//! In some cases, it may be helpful to split the ownership of a channel handle
//! into a sender and a receiver.

use std::pin::Pin;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use muzanci_interpreter::{EvalResult, Job, Pipeline, Step, StepId};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWrite;
use tokio::io::{self, AsyncRead, ReadBuf};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

use crate::codec::Frame;
use crate::mux::Command;
use crate::mux::MuxError;

pub type ChannelId = uuid::Uuid;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
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

    /// A workdir channel. Initiated by a client.
    Workdir,

    /// A tunnel channel. Initiated by a runner.
    Tunnel,
}

pub type RawData = Vec<u8>;

/// A message sent between peers on a channel.
/// Control messages are sent from a [`mux`] task to the peer mux task to manage channels. Control messages are always sent on the control channel ([`uuid::Uuid::nil`]).
/// Data messages are sent from a channel task to the peer channel task for application data exchange. Data messages are sent on the channel that they belong to.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    Control(ControlMessage),
    EvaluatorScheduler(EvaluatorSchedulerMessage),
    Evaluator(EvaluatorMessage),
    WorkerScheduler(WorkerSchedulerMessage),
    Worker(WorkerMessage),
    DebuggerScheduler(DebuggerSchedulerMessage),
    Debugger(DebuggerMessage),
    Workdir(WorkdirMessage),
    RawData(RawData),
}

/// Control messages are sent from a [`crate::mux::Mux<Stream, Acceptor>`] task to the peer mux task to manage channels. Control messages are always sent on the control channel ([`uuid::Uuid::nil`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlMessage {
    /// Requests the peer mux task to open a channel.
    /// The peer mux task must be constructed with a [`ChannelAcceptor`] that can handle the [`ControlMessage::OpenChannelRequest`] for the requested [`ChannelType`].
    /// If the peer mux task accepts the [`ControlMessage::OpenChannelRequest`], the peer mux will respond with an [`ControlMessage::OpenChannelResponse`] message containing an [`Ok`] result.
    /// If the peer mux task rejects the [`ControlMessage::OpenChannelRequest`], the peer mux will respond with an [`ControlMessage::OpenChannelResponse`] message containing an [`Err`] result.
    OpenChannelRequest {
        channel_id: ChannelId,
        channel_type: ChannelType,
    },
    /// Control message, response.
    /// Response to a [`ControlMessage::OpenChannelRequest`].
    OpenChannelResponse {
        channel_id: ChannelId,
        result: Result<(), String>,
    },
    CloseChannel {
        channel_id: ChannelId,
    },
}

pub type RunnerId = uuid::Uuid;

pub type TriggerId = uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaitingTrigger {
    pub trigger_id: TriggerId,
    pub capacity: u64,
}

pub type EvaluatorId = uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EvaluatorSchedulerMessage {
    FetchWaitingTriggersRequest,
    FetchWaitingTriggersResponse {
        result: Result<Vec<WaitingTrigger>, String>,
    },
    ReserveTriggerRequest {
        trigger_id: TriggerId,
    },
    ReserveTriggerResponse {
        result: Result<EvaluatorId, String>,
    },
}

pub type RepoUrl = url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EvaluatorMessage {
    StartRequest {
        evaluator_id: EvaluatorId,
    },
    StartResponse {
        result: Result<RepoUrl, String>,
    },
    CompleteRequest {
        evaluator_id: EvaluatorId,
        pipelines: Vec<Pipeline>,
        jobs: Vec<Job>,
    },
    CompleteResponse {
        result: Result<(), String>,
    },
    FailRequest {
        evaluator_id: EvaluatorId,
        reason: String,
    },
    FailResponse {
        result: Result<(), String>,
    },
}

pub type TaskId = uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaitingTask {
    pub task_id: TaskId,
    pub capacity: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerSchedulerMessage {
    // TODO: Enrich fetch with filters like capabilities and capacity.
    FetchWaitingTasksRequest,
    FetchWaitingTasksResponse {
        result: Result<Vec<WaitingTask>, String>,
    },
    ReserveTaskRequest {
        runner_id: RunnerId,
        task_id: TaskId,
    },
    ReserveTaskResponse {
        result: Result<(), String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerMessage {
    StartRequest {
        runner_id: RunnerId,
        task_id: TaskId,
    },
    StartResponse {
        result: Result<Vec<Step>, String>,
    },
    CompleteRequest {
        runner_id: RunnerId,
        task_id: TaskId,
    },
    CompleteResponse {
        result: Result<(), String>,
    },
    FailRequest {
        runner_id: RunnerId,
        task_id: TaskId,
        reason: String,
    },
    FailResponse {
        result: Result<(), String>,
    },
    StartStepRequest {
        runner_id: RunnerId,
        task_id: TaskId,
        step_id: StepId,
    },
    StartStepResponse {
        result: Result<(), String>,
    },
    CompleteStepRequest {
        runner_id: RunnerId,
        task_id: TaskId,
        step_id: StepId,
    },
    CompleteStepResponse {
        result: Result<(), String>,
    },
    FailStepRequest {
        runner_id: RunnerId,
        task_id: TaskId,
        step_id: StepId,
        reason: String,
    },
    FailStepResponse {
        result: Result<(), String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DebuggerSchedulerMessage {
    FetchWaitingWorkdirsRequest,
    FetchWaitingWorkdirsResponse,
    ReserveWorkdirRequest,
    ReserveWorkdirResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DebuggerMessage {
    StartSessionRequest,
    StartSessionResponse,
    FinishSessionRequest,
    FinishSessionResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkdirMessage {
    CreateWorkdirRequest,
    CreateWorkdirResponse,
}

/// A handle to a channel that can be used to send messages to the peer and receive messages from the peer.
/// The channel handle is owned by a single task.
/// In some cases, it may be useful to split the ownership of a channel handle into a sender and a receiver.
/// This can be done using [`ChannelHandle::take_message_rx`] to take the message receiver out of the channel handle and give it to another task.
pub struct ChannelSender {
    /// The channel's unique identifier.
    channel_id: ChannelId,

    channel_type: ChannelType,

    /// A sender for outbound frames, from a local task to the peer task.
    frame_tx: mpsc::Sender<Frame>,

    /// A sender for commands to the mux task, such as closing the channel.
    command_tx: mpsc::Sender<Command>,

    /// Whether or not the channel has been closed.
    closed: Arc<AtomicBool>,
}

pub struct ChannelReceiver {
    /// The channel's unique identifier.
    channel_id: ChannelId,

    channel_type: ChannelType,

    /// A receiver for inbound messages from the peer task.
    message_rx: mpsc::Receiver<Message>,

    /// A sender for commands to the mux task, such as closing the channel.
    command_tx: mpsc::Sender<Command>,

    /// Whether or not the channel has been closed.
    closed: Arc<AtomicBool>,
}

pub fn channel(
    channel_id: ChannelId,
    channel_type: ChannelType,
    frame_tx: mpsc::Sender<Frame>,
    command_tx: mpsc::Sender<Command>,
    message_rx: mpsc::Receiver<Message>,
    closed: Arc<AtomicBool>,
) -> (ChannelSender, ChannelReceiver) {
    let sender = ChannelSender {
        channel_id,
        channel_type,
        frame_tx,
        command_tx: command_tx.clone(),
        closed: closed.clone(),
    };
    let receiver = ChannelReceiver {
        channel_id,
        channel_type,
        message_rx,
        command_tx,
        closed,
    };
    (sender, receiver)
}

impl ChannelSender {
    /// Sends a [`Message`] to the peer.
    pub async fn send(&self, message: Message) -> Result<(), MuxError> {
        if self.closed.load(std::sync::atomic::Ordering::SeqCst) {
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
}

impl Drop for ChannelSender {
    fn drop(&mut self) {
        tracing::info!(
            "Dropping ChannelSender [{:?}][{}]",
            self.channel_type,
            self.channel_id
        );
        let closed = self
            .closed
            .fetch_or(true, std::sync::atomic::Ordering::SeqCst);
        if closed {
            tracing::info!(
                "ChannelSender [{:?}][{}] already closed, not sending close command",
                self.channel_type,
                self.channel_id
            );
            return;
        }

        if let Err(e) = self.command_tx.try_send(Command::CloseChannel {
            channel_id: self.channel_id,
        }) {
            tracing::error!(
                "Failed to send close command for channel_id {}: {}",
                self.channel_id,
                e
            );
        }
    }
}

impl ChannelReceiver {
    /// Receives a [`Message`] from the channel.
    /// Returns [`None`] if the channel has been closed.
    pub async fn recv(&mut self) -> Option<Message> {
        if self.closed.load(std::sync::atomic::Ordering::SeqCst) {
            tracing::info!(
                "ChannelReceiver [{:?}][{}] has been closed, returning None",
                self.channel_type,
                self.channel_id
            );
            return None;
        }

        match self.message_rx.recv().await {
            Some(message) => Some(message),

            None => {
                tracing::error!(
                    "ChannelReceiver [{:?}][{}] has been closed by the peer.",
                    self.channel_type,
                    self.channel_id,
                );
                None
            }
        }
    }
}

impl Drop for ChannelReceiver {
    fn drop(&mut self) {
        tracing::info!(
            "Dropping ChannelReceiver [{:?}][{}]",
            self.channel_type,
            self.channel_id
        );
        let closed = self
            .closed
            .fetch_or(true, std::sync::atomic::Ordering::SeqCst);

        if closed {
            tracing::info!(
                "ChannelReceiver [{:?}][{}] already closed, not sending close command",
                self.channel_type,
                self.channel_id
            );
            return;
        }

        tracing::info!(
            "ChannelReceiver [{:?}][{}] sending close command.",
            self.channel_type,
            self.channel_id
        );
        if let Err(e) = self.command_tx.try_send(Command::CloseChannel {
            channel_id: self.channel_id,
        }) {
            tracing::error!(
                "Failed to send close command for channel_id {}: {}",
                self.channel_id,
                e
            );
        }
    }
}

pub struct ChannelStream {
    tx: ChannelSender,
    rx: ChannelReceiver,
    read_buf: bytes::Bytes, // leftover bytes from a partially consumed Data message
}

impl ChannelStream {
    pub fn new(sender: ChannelSender, receiver: ChannelReceiver) -> Self {
        if sender.channel_id != receiver.channel_id {
            panic!(
                "ChannelSender and ChannelReceiver must have the same channel_id. Got {} and {}",
                sender.channel_id, receiver.channel_id
            );
        }
        ChannelStream {
            tx: sender,
            rx: receiver,
            read_buf: bytes::Bytes::new(),
        }
    }
}

impl AsyncRead for ChannelStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
        // Drain read_buf first
        if !self.read_buf.is_empty() {
            let n = buf.remaining().min(self.read_buf.len());
            buf.put_slice(&self.read_buf.split_to(n));
            return Poll::Ready(Ok(()));
        }
        // Poll the mpsc receiver
        match self.rx.message_rx.poll_recv(cx) {
            Poll::Ready(Some(Message::RawData(data))) => {
                let n = buf.remaining().min(data.len());
                buf.put_slice(&data[..n]);
                if n < data.len() {
                    self.read_buf = bytes::Bytes::from(data).slice(n..);
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(None) => {
                Poll::Ready(Ok(())) // EOF
            }
            Poll::Ready(Some(_)) => Poll::Pending, // control message, skip
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for ChannelStream {
    fn poll_write(self: Pin<&mut Self>, _: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        // Split into ≤4096-byte chunks and send each as a Data frame.
        let chunk = &buf[..buf.len().min(4096)];
        let frame = Frame {
            channel_id: self.tx.channel_id,
            message: Message::RawData(chunk.to_vec()),
        };
        match self.tx.frame_tx.try_send(frame) {
            Ok(()) => Poll::Ready(Ok(chunk.len())),
            Err(TrySendError::Full(_)) => {
                // Register a waker — the simplest approach is to use poll_reserve
                // on the Sender (available in Tokio's Sender::reserve)
                Poll::Pending
            }
            Err(TrySendError::Closed(_)) => {
                Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)))
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(())) // framed writer handles flushing
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// A function that accepts a channel handle and returns a future that sends
/// and receives messages on the channel.
pub type ChannelFutureFn =
    dyn FnOnce(ChannelSender, ChannelReceiver) -> BoxFuture<'static, ()> + Send;

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
    F: FnOnce(ChannelSender, ChannelReceiver) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Box::new(move |tx, rx| Box::pin(f(tx, rx)))
}
