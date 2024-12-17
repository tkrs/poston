use crate::buffer::{BufferError, Record, Take};
use crate::connect::*;
use base64::{engine::general_purpose, Engine as _};
use std::cell::RefCell;
use std::cmp;
use std::collections::VecDeque;
use std::time::SystemTime;
use uuid::Uuid;

#[derive(Debug, PartialEq)]
pub struct Emitter {
    tag: String,
    queue: RefCell<VecDeque<(SystemTime, Vec<u8>)>>,
}

impl Emitter {
    pub fn new(tag: String) -> Self {
        let queue = RefCell::new(VecDeque::new());
        Self { tag, queue }
    }

    pub fn len(&self) -> usize {
        self.queue.borrow().len()
    }

    pub fn push(&self, elem: (SystemTime, Vec<u8>)) {
        let mut q = self.queue.borrow_mut();
        q.push_back(elem)
    }

    pub fn emit<RW: WriteRead>(
        &self,
        rw: &mut RW,
        size: Option<usize>,
    ) -> Result<(), EmitterError> {
        let mut queue = self.queue.borrow_mut();
        if queue.is_empty() {
            return Ok(());
        }
        let qlen = queue.len();
        let mut entries = Vec::with_capacity(cmp::min(qlen, size.unwrap_or(qlen)));
        queue.take(&mut entries);

        let chunk = general_purpose::STANDARD.encode(Uuid::new_v4().to_string());
        let rec = Record::new(&self.tag, entries.as_slice(), &chunk);
        let buf = rec.pack().map_err(EmitterError::Buffer)?;
        rw.write_and_read(&buf, &chunk)
            .map_err(EmitterError::Stream)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum EmitterError {
    #[error("buffer error")]
    Buffer(#[from] BufferError),
    #[error("stream error")]
    Stream(#[from] StreamError),
}

#[cfg(test)]
mod test {
    use super::*;

    struct TestStream;

    impl WriteRead for TestStream {
        fn write_and_read(&mut self, _buf: &[u8], _chunk: &str) -> Result<(), StreamError> {
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
        fn write_and_read(&mut self, _buf: &[u8], _chunk: &str) -> Result<(), StreamError> {
            Err(StreamError::AckUmatched("a".to_string(), "b".to_string()))
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

    #[test]
    fn emit_valid_chunks() {
        struct Mock {
            acc: RefCell<Vec<(Vec<u8>, String)>>,
        }

        impl Mock {
            pub fn new() -> Mock {
                Mock {
                    acc: RefCell::new(Vec::new()),
                }
            }
        }

        impl WriteRead for Mock {
            fn write_and_read(&mut self, buf: &[u8], chunk: &str) -> Result<(), StreamError> {
                let mut acc = self.acc.borrow_mut();
                let args = (buf.to_vec(), chunk.to_string());
                acc.push(args);
                Ok(())
            }
        }

        let emitter = Emitter::new("x".to_string());

        for i in 1..1000 {
            emitter.push((SystemTime::now(), vec![0x00, (i as u8)]));
        }

        let rw = &mut Mock::new();
        emitter.emit(rw, Some(2)).unwrap();

        let acc = rw.acc.borrow();
        let chunks = acc
            .iter()
            .map(|(_, v)| general_purpose::STANDARD.decode(v).unwrap())
            .collect::<Vec<Vec<u8>>>();

        for chunk in chunks.iter() {
            let _ = Uuid::parse_str(&String::from_utf8(chunk.clone()).unwrap()).unwrap();
        }
    }
}
