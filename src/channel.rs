//! A channel is a bidirectional data stream between two peers.
//! A channel does not support broadcasting. A channel handle should only
//! have a single owner.
//!
//! In some cases, it may be helpful to split the ownership of a channel handle
//! into a sender and a receiver.

use futures::future::BoxFuture;
use tokio::sync::mpsc;

use crate::job::{AvailableJob, JobId};
use crate::mux::Command;

use crate::codec::Frame;
use crate::mux::MuxError;
use crate::worker::{WorkerConfig, WorkerEvent, WorkerId};

pub type ChannelId = uuid::Uuid;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ChannelType {
    /// A scheduler channel. Initiated by a runner to query and acquire jobs from the server.
    Scheduler,

    /// A worker channel. Initiated by a runner to initialize a worker, send worker lifecycle events, and receive timeout events.
    Worker,

    /// A tunnel channel. Initiated by a server to establish an SSH tunnel to a runner for debugging purposes.
    Tunnel,
}

/// A message sent between peers on a channel.
/// Control messages are sent from a [`mux`] task to the peer mux task to manage channels. Control messages are always sent on the control channel ([`uuid::Uuid::nil`]).
/// Data messages are sent from a channel task to the peer channel task for application data exchange. Data messages are sent on the channel that they belong to.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Message {
    /// Control message, request.
    /// Requests the peer mux task to open a channel.
    /// The peer mux task must be constructed with a [`ChannelAcceptor`] that can handle the [`Message::OpenChannelRequest`] for the requested [`ChannelType`].
    /// If the peer mux task accepts the [`Message::OpenChannelRequest`], the peer mux will respond with an [`Message::OpenChannelResponse`] message containing an [`Ok`] result.
    /// If the peer mux task rejects the [`Message::OpenChannelRequest`], the peer mux will respond with an [`Message::OpenChannelResponse`] message containing an [`Err`] result.
    OpenChannelRequest {
        channel_id: ChannelId,
        channel_type: ChannelType,
        buffer_size: usize,
    },

    /// Control message, response.
    /// Response to an [`Message::OpenChannelRequest`].
    OpenChannelResponse {
        channel_id: ChannelId,
        result: Result<(), String>,
    },

    /// Control message, fire-and-forget.
    /// Requests the peer mux task to close the channel. No response should be expected.
    CloseChannel { channel_id: ChannelId },

    /// Data message, request, scheduler channel.
    /// Requests the peer scheduler channel task to query available jobs.
    QueryAvailableJobsRequest,

    /// Data message, response, scheduler channel.
    /// Response to a [`Message::QueryAvailableJobsRequest`].
    QueryAvailableJobsResponse { available_jobs: Vec<AvailableJob> },

    /// Data message, request, scheduler channel.
    /// Requests the peer scheduler channel task to acquire a job.
    AcquireJobRequest { job_id: JobId },

    /// Data message, response, scheduler channel.
    /// Response to an [`Message::AcquireJobRequest`].
    AcquireJobResponse {
        job_id: JobId,
        result: Result<WorkerId, String>,
    },

    /// Data message, request, worker channel.
    /// Requests the peer worker channel task to provide the configuration for a worker.
    WorkerConfigRequest { worker_id: WorkerId },

    /// Data message, response, worker channel.
    /// Response to a [`Message::WorkerConfigRequest`].
    WorkerConfigResponse(Result<WorkerConfig, String>),

    /// Data message, fire-and-forget, worker channel.
    /// Notifies the peer worker channel task about a worker lifecycle event, such as starting or completing a job.
    WorkerEvent(WorkerEvent),

    /// Data message, fire-and-forget, worker channel.
    /// Notifies the peer worker channel task that the worker has timed out.
    WorkerTimedOut,
}

/// The state of a channel.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ChannelState {
    Open,
    Closed,
}

/// A handle to a channel that can be used to send messages to the peer and receive messages from the peer.
/// The channel handle is owned by a single task.
/// In some cases, it may be useful to split the ownership of a channel handle into a sender and a receiver.
/// This can be done using [`ChannelHandle::take_message_rx`] to take the message receiver out of the channel handle and give it to another task.
pub struct ChannelHandle {
    /// The channel's unique identifier.
    channel_id: ChannelId,

    /// A sender for outbound frames, from a local task to the peer task.
    frame_tx: mpsc::Sender<Frame>,

    /// A sender for commands to the mux task, such as closing the channel.
    command_tx: mpsc::Sender<Command>,

    /// A receiver for inbound messages from the peer.
    message_rx: Option<mpsc::Receiver<Message>>,

    /// The state of the channel.
    state: ChannelState,
}

impl ChannelHandle {
    /// Constructs a new [`ChannelHandle`].
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
            message_rx: Some(message_rx),
            state,
        }
    }

    /// Sends a [`Message`] to the peer.
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
    /// Returns [`None`] if the channel has been closed by the peer.
    pub async fn recv(&mut self) -> Option<Message> {
        let message_rx = match self.message_rx.as_mut() {
            Some(rx) => rx,
            None => {
                panic!(
                    "Channel {} message receiver is already taken. Channel handle cannot be used to receive messages anymore.",
                    self.channel_id
                );
            }
        };

        match message_rx.recv().await {
            Some(message) => Some(message),

            // Local mux task has terminated.
            None => {
                self.state = ChannelState::Closed;
                None
            }
        }
    }

    /// Takes the message receiver out of the channel handle and returns it.
    /// This is useful for transferring the ownership of the message receiver to another task.
    /// After calling this method, the channel handle can no longer be used to receive messages.
    pub fn take_message_rx(&mut self) -> mpsc::Receiver<Message> {
        match self.message_rx.take() {
            Some(rx) => rx,
            None => {
                panic!(
                    "Channel {} message receiver is already taken. Cannot be taken again.",
                    self.channel_id
                );
            }
        }
    }

    /// Closes the channel. After calling this method, the channel handle can no longer be used to send or receive messages.
    pub async fn close(&mut self) -> Result<(), MuxError> {
        if self.state == ChannelState::Closed {
            return Ok(());
        }
        self.state = ChannelState::Closed;

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

        if let Err(e) = self.command_tx.try_send(Command::CloseChannel {
            channel_id: self.channel_id,
        }) {
            eprintln!(
                "Failed to send close command for channel_id {}: {}",
                self.channel_id, e
            );
        }
    }
}

/// A function that accepts a channel handle and returns a future that sends
/// and receives messages on the channel.
pub type ChannelFutureFn = dyn FnOnce(ChannelHandle) -> BoxFuture<'static, ()> + Send;

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
    F: FnOnce(ChannelHandle) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Box::new(move |handle| Box::pin(f(handle)))
}
