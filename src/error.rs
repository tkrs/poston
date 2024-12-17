use crossbeam_channel::{RecvError, SendError};

use crate::{buffer::BufferError, worker::Message};

#[derive(thiserror::Error, Debug)]
pub enum ClientError {
    #[error("buffer error")]
    Buffer(#[from] BufferError),
    #[error("send error")]
    SendChannel(#[from] SendError<Message>),
    #[error("receive error")]
    RecieveChannel(#[from] RecvError),
}
