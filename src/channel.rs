use futures::future::BoxFuture;
use tokio::sync::mpsc;

use crate::mux::Command;

use crate::mux::{Frame, MuxError};
use crate::worker::WorkerId;

pub type ChannelId = uuid::Uuid;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ChannelType {
    Scheduler,
    Worker,
    Tunnel,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum WorkerEvent {
    Started,
    StdoutLine,
    StderrLine,
    Exited,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkerConfig {
    worker_id: u64,
    repo_owner: String,
    repo_name: String,
    commit_sha: String,
    worker_capacity: u64,
}

impl WorkerConfig {
    pub fn worker_id(&self) -> u64 {
        self.worker_id
    }

    pub fn repo_owner(&self) -> &str {
        &self.repo_owner
    }

    pub fn repo_name(&self) -> &str {
        &self.repo_name
    }

    pub fn commit_sha(&self) -> &str {
        &self.commit_sha
    }

    pub fn worker_capacity(&self) -> u64 {
        self.worker_capacity
    }
}

pub type JobId = uuid::Uuid;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AvailableJob {
    job_id: JobId,
    runner_capacity_required: u64,
}

impl AvailableJob {
    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    pub fn runner_capacity_required(&self) -> u64 {
        self.runner_capacity_required
    }
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
        result: Result<(), String>,
    },

    InitializeWorkerRequest {
        worker_id: WorkerId,
    },

    InitializeWorkerResponse(Result<WorkerConfig, String>),

    /// A control message
    /// A lifecycle event for a Worker.
    WorkerEvent(WorkerEvent),
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

        // Delete channel from the mux's dispatch table.
        let _ = self.command_tx.try_send(Command::CloseChannel {
            channel_id: self.channel_id,
        });
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
