pub mod channel;
pub mod codec;
pub mod message;
pub mod mux;
// pub mod mux2;

// pub use mux2 as mux;

/// The HTTP Upgrade header value for the MuzanCI transport protocol.
pub const MUZANCI_TRANSPORT_V1: &str = "muzanci-transport/v1";

/// The HTTP header for the runner ID, sent by the server upon successful upgrade to the MuzanCI transport protocol.
/// The value must be a valid [`RunnerId`].
pub const MUZANCI_RUNNER_ID_HEADER: &str = "X-MuzanCI-Runner-ID";
