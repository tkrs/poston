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
    pub fn new(tag: String) -> Emitter {
        let queue = RefCell::new(VecDeque::new());
        Emitter { tag, queue }
    }

    pub fn push(&self, elem: (SystemTime, Vec<u8>)) {
        let mut q = self.queue.borrow_mut();
        q.push_back(elem)
    }

    pub fn emit<RW>(&self, rw: &mut RW, size: Option<usize>) -> Result<(), Error>
    where
        RW: ReconnectWrite + Read,
    {
        let chunk = base64::encode(&Uuid::new_v4().to_string());
        let mut buf = Vec::new();

        let mut queue = self.queue.borrow_mut();
        if queue.is_empty() {
            return Ok(());
        }
        let size = size.unwrap_or_else(|| queue.len());
        let mut entries = Vec::with_capacity(size);
        queue.take(&mut entries);

        buffer::pack_record(&mut buf, self.tag.clone(), entries, chunk.clone())?;

        rw.write(buf).map_err(Error::NetworkError)?;
        let mut resp_buf = [0u8; 64];
        let resp_size = rw.read(&mut resp_buf).map_err(Error::NetworkError)?;

        let reply = buffer::unpack_response(&resp_buf, resp_size)?;
        if reply.ack == chunk {
            Ok(())
        } else {
            Err(Error::AckUmatchedError)
        }
    }
}
