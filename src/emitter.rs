use crate::buffer::{self, Take};
use crate::connect::*;
use crate::error::Error;
use std::cell::RefCell;
use std::collections::VecDeque;
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

    pub fn len(&self) -> usize {
        self.queue.borrow().len()
    }

    pub fn push(&self, elem: (SystemTime, Vec<u8>)) {
        let mut q = self.queue.borrow_mut();
        q.push_back(elem)
    }

    pub fn emit<RW: WriteRead>(&self, rw: &mut RW, size: Option<usize>) -> Result<(), Error> {
        let mut queue = self.queue.borrow_mut();
        if queue.is_empty() {
            return Ok(());
        }
        let q_size = queue.len();
        let size = size.unwrap_or_else(|| q_size);
        let size = if q_size < size { q_size } else { size };

        let mut entries = Vec::with_capacity(size);
        queue.take(&mut entries);

        let chunk = base64::encode(&Uuid::new_v4().to_string());

        let mut buf = Vec::new();
        buffer::pack_record(&mut buf, self.tag.as_str(), entries, chunk.as_str())?;
        rw.write_and_read(&buf, &chunk)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    struct TestStream;

    impl WriteRead for TestStream {
        fn write_and_read(&mut self, _buf: &[u8], _chunk: &str) -> Result<(), Error> {
            Ok(())
        }
    }

    #[test]
    fn emit_consume_queeu() {
        let emitter = Emitter::new("x".to_string());

        emitter.push((SystemTime::now(), vec![0x00, 0x01]));
        emitter.push((SystemTime::now(), vec![0x00, 0x02]));
        emitter.push((SystemTime::now(), vec![0x00, 0x03]));
        emitter.push((SystemTime::now(), vec![0x00, 0x04]));
        emitter.push((SystemTime::now(), vec![0x00, 0x05]));

        {
            emitter.emit(&mut TestStream, Some(3)).unwrap();
            let q = emitter.queue.borrow_mut();

            assert_eq!(q.len(), 2);
        }

        {
            emitter.emit(&mut TestStream, Some(3)).unwrap();
            let q = emitter.queue.borrow_mut();

            assert_eq!(q.len(), 0);
        }

        {
            emitter.emit(&mut TestStream, Some(3)).unwrap();
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
            emitter.emit(&mut TestStream, None).unwrap();
            let q = emitter.queue.borrow_mut();

            assert_eq!(q.len(), 0);
        }
    }

    struct TestErrStream;

    impl WriteRead for TestErrStream {
        fn write_and_read(&mut self, _buf: &[u8], _chunk: &str) -> Result<(), Error> {
            Err(Error::AckUmatchedError("a".to_string(), "b".to_string()))
        }
    }

    #[test]
    fn emit_consume_queue_with_error_stream() {
        let emitter = Emitter::new("x".to_string());

        emitter.push((SystemTime::now(), vec![0x00, 0x01]));
        emitter.push((SystemTime::now(), vec![0x00, 0x02]));
        emitter.push((SystemTime::now(), vec![0x00, 0x03]));
        emitter.push((SystemTime::now(), vec![0x00, 0x04]));
        emitter.push((SystemTime::now(), vec![0x00, 0x05]));

        assert!(emitter.emit(&mut TestErrStream, Some(3)).is_err());
        let q = emitter.queue.borrow_mut();

        assert_eq!(q.len(), 2);
    }
}
