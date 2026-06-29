use serde::{Deserialize, Serialize};

pub mod channel;
pub mod codec;
pub mod mux;

/// The HTTP Upgrade header value for the MuzanCI transport protocol.
pub const MUZANCI_TRANSPORT_V1: &str = "muzanci-transport/v1";

/// The HTTP header for the runner ID, sent by the server upon successful upgrade to the MuzanCI transport protocol.
/// The value must be a valid [`RunnerId`].
pub const MUZANCI_RUNNER_ID_HEADER: &str = "X-MuzanCI-Runner-ID";

pub type RunnerId = uuid::Uuid;
pub type WorkerId = uuid::Uuid;
