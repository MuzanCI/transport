use std::path::PathBuf;

use muzanci_git::GitBranch;
use muzanci_git::GitCommitSha;
use muzanci_image::image::ImagePlatform;
use muzanci_image::manifest_ref::ManifestRef;
use serde::Deserialize;
use serde::Serialize;
use url::Url;

use muzanci_interpreter::Config;
use muzanci_interpreter::StepConfig;
use muzanci_interpreter::StepId;

use crate::channel::ChannelId;
use crate::channel::ChannelType;

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
    RawData(RawData),
}

pub type RawData = Vec<u8>;

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

pub type GitCloneUrl = Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckoutConfig {
    pub url: GitCloneUrl,
    pub branch: GitBranch,
    pub commit_sha: GitCommitSha,
}

pub type TriggerId = uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaitingTrigger {
    pub trigger_id: TriggerId,
    pub capacity: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EvaluatorSchedulerMessage {
    FetchWaitingTriggersRequest,
    FetchWaitingTriggersResponse {
        result: Result<Vec<WaitingTrigger>, String>,
    },
    ReserveTriggerRequest {
        runner_id: RunnerId,
        trigger_id: TriggerId,
    },
    ReserveTriggerResponse {
        result: Result<(), String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProcessOutput {
    Stdout { index: usize, line: String },
    Stderr { index: usize, line: String },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ExitStatus {
    Code(i32),
    Signal,
}

impl ToString for ExitStatus {
    fn to_string(&self) -> String {
        match *self {
            ExitStatus::Code(code) => code.to_string(),
            ExitStatus::Signal => "signal".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluationConfig {
    pub checkout: CheckoutConfig,
    pub input: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EvaluatorMessage {
    StartRequest {
        runner_id: RunnerId,
        trigger_id: TriggerId,
    },
    StartResponse {
        result: Result<EvaluationConfig, String>,
    },
    CompleteRequest {
        runner_id: RunnerId,
        trigger_id: TriggerId,
        config: Config,
    },
    CompleteResponse {
        result: Result<(), String>,
    },
    FailRequest {
        runner_id: RunnerId,
        trigger_id: TriggerId,
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
    pub manifest_ref: ManifestRef,
    pub platform: ImagePlatform,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskConfig {
    pub checkout_config: CheckoutConfig,
    pub steps: Vec<StepConfig>,
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
        result: Result<TaskConfig, String>,
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
    StepProcessOutput {
        runner_id: RunnerId,
        task_id: TaskId,
        step_id: StepId,
        output: ProcessOutput,
    },
    StepProcessExitStatus {
        runner_id: RunnerId,
        task_id: TaskId,
        step_id: StepId,
        exit_status: ExitStatus,
    },
}
