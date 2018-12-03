use backoff::{Error as RetryError, ExponentialBackoff, Operation};
use base64;
use buffer::{self, Take};
use connect::ReconnectWrite;
use error::Error;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::Read;
use std::time::SystemTime;
use uuid::Uuid;

pub struct Emitter {
    tag: String,
    queue: RefCell<VecDeque<(SystemTime, Vec<u8>)>>,
}

impl Emitter {
    pub fn new(tag: String) -> Self {
        let queue = RefCell::new(VecDeque::new());
        Emitter { tag, queue }
    }

    pub fn push(&self, elem: (SystemTime, Vec<u8>)) {
        let mut q = self.queue.borrow_mut();
        q.push_back(elem)
    }

    pub fn emit<RW>(&self, rw: &mut RW, size: Option<usize>)
    where
        RW: ReconnectWrite + Read,
    {
        let chunk = base64::encode(&Uuid::new_v4().to_string());
        let mut buf = Vec::new();

        let mut queue = self.queue.borrow_mut();
        if queue.is_empty() {
            return;
        }
        let q_size = queue.len();
        let size = size.unwrap_or_else(|| q_size);
        let size = if q_size < size { q_size } else { size };
        let mut entries = Vec::with_capacity(size);
        queue.take(&mut entries);

        let _ = buffer::pack_record(&mut buf, self.tag.as_str(), entries, chunk.as_str());
        if let Err(err) = write_and_read(rw, &buf, &chunk) {
            error!(
                "Tag '{}', an unexpected error occurred during emitting message: '{:?}'.",
                self.tag, err
            );
        }
    }
}

fn write_and_read<RW>(rw: &mut RW, buf: &[u8], chunk: &str) -> Result<(), RetryError<Error>>
where
    RW: ReconnectWrite + Read,
{
    let mut op = || {
        rw.write(buf.to_owned())
            .map_err(Error::NetworkError)
            .map_err(RetryError::Transient)?;
        let mut resp_buf = [0u8; 64];
        let to_write = rw
            .read(&mut resp_buf)
            .map_err(Error::NetworkError)
            .map_err(RetryError::Transient)?;
        if to_write == 0 {
            Err(RetryError::Transient(Error::NoAckResponseError))
        } else {
            let reply =
                buffer::unpack_response(&resp_buf, to_write).map_err(RetryError::Transient)?;
            if reply.ack == chunk {
                Ok(())
            } else {
                Err(RetryError::Transient(Error::AckUmatchedError(
                    reply.ack,
                    chunk.to_string(),
                )))
            }
        }
    };
    let mut backoff = ExponentialBackoff::default(); // TODO: Should be configurable.
    op.retry(&mut backoff)
}

#[cfg(test)]
mod test {}
