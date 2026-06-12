use futures::future::BoxFuture;
use tokio::sync::mpsc;

use crate::job::{AvailableJob, JobId};
use crate::mux::Command;

use crate::mux::{Frame, MuxError};
use crate::worker::{WorkerConfig, WorkerEvent, WorkerId};

pub type ChannelId = uuid::Uuid;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ChannelType {
    Scheduler,
    Worker,
    Tunnel,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Message {
    /// A packet of data.
    Data(#[serde(with = "serde_bytes")] Vec<u8>),

    /// A control message to open a channel.
    OpenChannelRequest {
        channel_id: ChannelId,
        channel_type: ChannelType,
        buffer_size: usize,
    },

    /// A control message to close a channel.
    CloseChannel {
        channel_id: ChannelId,
    },

    /// A control message to acknowledge successful channel open.
    OpenChannelResponse {
        channel_id: ChannelId,
        result: Result<(), String>,
    },

    QueryAvailableJobsRequest,
    QueryAvailableJobsResponse {
        available_jobs: Vec<AvailableJob>,
    },

    AcquireJobRequest {
        job_id: JobId,
    },

    AcquireJobResponse {
        job_id: JobId,
        result: Result<WorkerId, String>,
    },

    WorkerConfigRequest {
        worker_id: WorkerId,
    },

    WorkerConfigResponse(Result<WorkerConfig, String>),

    /// A lifecycle event for a Worker.
    WorkerEvent(WorkerEvent),

    WorkerTimedOut,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ChannelState {
    Open,
    Closed,
}

pub struct ChannelHandle {
    /// The channel's unique identifier.
    channel_id: ChannelId,

    /// A channel for outbound frames, from a local task to the peer.
    frame_tx: mpsc::Sender<Frame>,

    /// A channel for commands to the mux task, such as closing the channel.
    command_tx: mpsc::Sender<Command>,

    /// A channel for inbound messages from the peer.
    message_rx: Option<mpsc::Receiver<Message>>,

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
            message_rx: Some(message_rx),
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
        let message_rx = match self.message_rx.as_mut() {
            Some(rx) => rx,
            None => {
                panic!(
                    "Channel {} message receiver is already taken. Channel handle cannot be used to receive messages anymore.",
                    self.channel_id
                );
            }
        };

        let message = match message_rx.recv().await {
            // Message received.
            Some(message) => message,

            // Mux task has terminated.
            None => {
                self.state = ChannelState::Closed;
                return None;
            }
        };

        Some(message)
    }

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
        eprintln!(
            "Dropping channel handle for channel_id: {}",
            self.channel_id
        );
        if self.state == ChannelState::Closed {
            eprintln!(
                "Channel {} is already closed. No need to send close command.",
                self.channel_id
            );
            return;
        }
        self.state = ChannelState::Closed;

        eprintln!("Sending close command for channel_id: {}", self.channel_id);
        // Delete channel from the mux's dispatch table.
        let result = self.command_tx.try_send(Command::CloseChannel {
            channel_id: self.channel_id,
        });
        if let Err(e) = result {
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
