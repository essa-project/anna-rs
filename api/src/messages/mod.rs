//! Defines the message types that are sent between nodes and threads.

pub use self::{
    addr_request::AddressRequest,
    addr_response::{AddressResponse, KeyAddress},
    request::Request,
    response::Response,
};

mod addr_request;
mod addr_response;
pub mod request;
pub mod response;

/// The message type that anna nodes send over TCP.
///
/// This type is only used for TCP messages. For messages sent over `zenoh`,
/// the wrapped inner types are used directly.
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TcpMessage {
    /// Ping message to test latency.
    ///
    /// The receiver should respond with a `Pong` message.
    Ping {
        /// The payload that the receiver should respond with.
        payload: Vec<u8>,
    },
    /// Reply to a `Ping` message.
    Pong {
        /// Sends back the payload given in the `Ping` message.
        payload: Vec<u8>,
    },
    /// An [`AddressRequest`] message.
    AddressRequest(AddressRequest),
    /// An [`AddressResponse`] message.
    AddressResponse(AddressResponse),
    /// A [`Request`] message.
    Request(Request),
    /// A [`Response`] message.
    Response(Response),
}
