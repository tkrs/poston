use crate::buffer::{self, Take};
use crate::connect::*;
use base64;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::time::SystemTime;
use uuid::Uuid;

pub struct Emitter {
    id: usize,
    tag: String,
    queue: RefCell<VecDeque<(SystemTime, Vec<u8>)>>,
}

impl Emitter {
    pub fn new(id: usize, tag: String) -> Self {
        let queue = RefCell::new(VecDeque::new());
        Emitter { id, tag, queue }
    }

    pub fn push(&self, elem: (SystemTime, Vec<u8>)) {
        let mut q = self.queue.borrow_mut();
        q.push_back(elem)
    }

    pub fn emit<RW: WriteRead>(&self, rw: &mut RW, size: Option<usize>) {
        let chunk = base64::encode(&Uuid::new_v4().to_string());
        let mut buf = Vec::new();

        let mut queue = self.queue.borrow_mut();
        if queue.is_empty() {
            return;
        }
        let q_size = queue.len();
        let size = size.unwrap_or_else(|| q_size);
        let size = if q_size < size { q_size } else { size };
        trace!(
            "Worker {} Consuming entries; size: {} / {}",
            self.id,
            size,
            q_size
        );
        let mut entries = Vec::with_capacity(size);
        queue.take(&mut entries);

        let _ = buffer::pack_record(&mut buf, self.tag.as_str(), entries, chunk.as_str());
        if let Err(err) = rw.write_and_read(&buf, &chunk) {
            error!(
                "Tag '{}', an unexpected error occurred during emitting message: '{:?}'.",
                self.tag, err
            );
        }
    }
}

#[cfg(test)]
mod test {}
