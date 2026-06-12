pub mod channel;
pub mod codec;
pub mod job;
pub mod mux;
pub mod runner;
pub mod worker;

pub const MUZANCI_TRANSPORT_V1: &str = "muzanci-transport/v1";
pub const MUZANCI_RUNNER_ID_HEADER: &str = "X-MuzanCI-Runner-ID";
