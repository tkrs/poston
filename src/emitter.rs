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
        let mut queue = self.queue.borrow_mut();
        if queue.is_empty() {
            return;
        }
        let q_size = queue.len();
        let size = size.unwrap_or_else(|| q_size);
        let size = if q_size < size { q_size } else { size };

        trace!("Worker {} consuming entries: {}/{}", self.id, size, q_size);

        let mut entries = Vec::with_capacity(size);
        queue.take(&mut entries);

        let mut buf = Vec::new();
        let chunk = base64::encode(&Uuid::new_v4().to_string());

        let _ = buffer::pack_record(&mut buf, self.tag.as_str(), entries, chunk.as_str());
        if let Err(err) = rw.write_and_read(&buf, &chunk) {
            error!(
                "Worker '{}' tag '{}' unexpected error occurred during emitting message: '{:?}'.",
                self.id, self.tag, err
            );
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::error::Error as MyError;
    use backoff::Error;

    struct TestStream;

    impl WriteRead for TestStream {
        fn write_and_read(&mut self, _buf: &[u8], _chunk: &str) -> Result<(), Error<MyError>> {
            Ok(())
        }
    }

    #[test]
    fn emit_consume_queeu() {
        let emitter = Emitter::new(1, "x".to_string());

        emitter.push((SystemTime::now(), vec![0x00, 0x01]));
        emitter.push((SystemTime::now(), vec![0x00, 0x02]));
        emitter.push((SystemTime::now(), vec![0x00, 0x03]));
        emitter.push((SystemTime::now(), vec![0x00, 0x04]));
        emitter.push((SystemTime::now(), vec![0x00, 0x05]));

        {
            emitter.emit(&mut TestStream, Some(3));
            let q = emitter.queue.borrow_mut();

            assert_eq!(q.len(), 2);
        }

        {
            emitter.emit(&mut TestStream, Some(3));
            let q = emitter.queue.borrow_mut();

            assert_eq!(q.len(), 0);
        }

        {
            emitter.emit(&mut TestStream, Some(3));
            let q = emitter.queue.borrow_mut();

            assert_eq!(q.len(), 0);
        }

        emitter.push((SystemTime::now(), vec![0x00, 0x01]));
        emitter.push((SystemTime::now(), vec![0x00, 0x02]));
        emitter.push((SystemTime::now(), vec![0x00, 0x03]));
        emitter.push((SystemTime::now(), vec![0x00, 0x04]));
        emitter.push((SystemTime::now(), vec![0x00, 0x05]));

        {
            let q = emitter.queue.borrow_mut();

            assert_eq!(q.len(), 5);
        }

        {
            emitter.emit(&mut TestStream, None);
            let q = emitter.queue.borrow_mut();

            assert_eq!(q.len(), 0);
        }
    }

    struct TestErrStream;

    impl WriteRead for TestErrStream {
        fn write_and_read(&mut self, _buf: &[u8], _chunk: &str) -> Result<(), Error<MyError>> {
            Err(Error::Permanent(MyError::AckUmatchedError(
                "a".to_string(),
                "b".to_string(),
            )))
        }
    }

    #[test]
    fn emit_consume_queue_with_error_stream() {
        let emitter = Emitter::new(1, "x".to_string());

        emitter.push((SystemTime::now(), vec![0x00, 0x01]));
        emitter.push((SystemTime::now(), vec![0x00, 0x02]));
        emitter.push((SystemTime::now(), vec![0x00, 0x03]));
        emitter.push((SystemTime::now(), vec![0x00, 0x04]));
        emitter.push((SystemTime::now(), vec![0x00, 0x05]));

        emitter.emit(&mut TestErrStream, Some(3));
        let q = emitter.queue.borrow_mut();

        assert_eq!(q.len(), 2);
    }
}
