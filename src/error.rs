use std::error::Error as StdError;
use std::fmt;
use std::io;

#[derive(Debug)]
pub enum Error {
    NetworkError(io::Error),
    DeriveError(String),
    SendError(String),
    AckUmatchedError(String, String),
    EmittingTimeoutError,
    ConnectingTimeoutError,
    NoAckResponseError,
}

impl StdError for Error {
    fn description(&self) -> &str {
        match *self {
            Error::NetworkError(ref e) => e.description(),
            Error::DeriveError(ref e) => e,
            Error::SendError(ref e) => e,
            Error::AckUmatchedError(_, _) => "request chunk and response ack-id did not match",
            Error::EmittingTimeoutError => "emitting timeout",
            Error::ConnectingTimeoutError => "connecting timeout",
            Error::NoAckResponseError => "no ack response",
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}
