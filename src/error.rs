use std::error::Error as StdError;
use std::fmt;

#[derive(Debug)]
pub enum Error {
    Network(String),
    Derive(String),
    Send(String),
    Terminate(String),
    AckUmatched(String, String),
    EmittingTimeout,
    ConnectingTimeout,
    NoAckResponse,
}

impl StdError for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match *self {
            Error::Network(ref e) => e,
            Error::Derive(ref e) => e,
            Error::Send(ref e) => e,
            Error::Terminate(ref e) => e,
            Error::AckUmatched(_, _) => "request chunk and response ack-id did not match",
            Error::EmittingTimeout => "emitting timeout",
            Error::ConnectingTimeout => "connecting timeout",
            Error::NoAckResponse => "no ack response",
        };
        write!(f, "{}", s)
    }
}
