use std::error::Error as StdError;
use std::fmt;
use std::io;

#[derive(Debug)]
pub enum Error {
    NetworkError(io::Error),
    DeriveError(String),
    SendError(String),
    AckUmatchedError,
    EmittingTimeoutError,
    ConnectingTimeoutError,
}

impl StdError for Error {
    fn description(&self) -> &str {
        match self {
            &Error::NetworkError(ref e) => e.description(),
            &Error::DeriveError(ref e) => e,
            &Error::SendError(ref e) => e,
            &Error::AckUmatchedError => "request chunk and response ack-id did not match",
            &Error::EmittingTimeoutError => "emitting timeout",
            &Error::ConnectingTimeoutError => "connecting timeout",
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}
