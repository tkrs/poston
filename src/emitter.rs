use crate::buffer::{BufferError, Record, Take};
use crate::connect::*;
use base64::{engine::general_purpose, Engine as _};
use std::cmp;
use std::collections::VecDeque;
use std::time::SystemTime;
use uuid::Uuid;

#[derive(Debug, PartialEq)]
pub struct Emitter {
    tag: String,
    queue: VecDeque<(SystemTime, Vec<u8>)>,
}

impl Emitter {
    pub fn new(tag: String) -> Self {
        let queue = VecDeque::new();
        Self { tag, queue }
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn push(&mut self, elem: (SystemTime, Vec<u8>)) {
        self.queue.push_back(elem)
    }

    pub fn emit<RW: WriteRead>(
        &mut self,
        rw: &mut RW,
        size: Option<usize>,
        does_recover: bool,
    ) -> Result<(), EmitterError> {
        if self.queue.is_empty() {
            return Ok(());
        }
        let qlen = self.queue.len();
        let mut entries = Vec::with_capacity(cmp::min(qlen, size.unwrap_or(qlen)));
        self.queue.take(&mut entries);

        let chunk = general_purpose::STANDARD.encode(Uuid::new_v4().to_string());
        let rec = Record::new(&self.tag, entries.as_slice(), &chunk);
        let buf = rec.pack().map_err(EmitterError::Buffer)?;
        rw.write_and_read(&buf, &chunk).map_err(|e| {
            if does_recover {
                self.queue.extend(entries);
            }
            EmitterError::Stream(e)
        })
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
        let mut emitter = Emitter::new("x".to_string());

        emitter.push((SystemTime::now(), vec![0x00, 0x01]));
        emitter.push((SystemTime::now(), vec![0x00, 0x02]));
        emitter.push((SystemTime::now(), vec![0x00, 0x03]));
        emitter.push((SystemTime::now(), vec![0x00, 0x04]));
        emitter.push((SystemTime::now(), vec![0x00, 0x05]));

        {
            emitter.emit(&mut TestStream, Some(3), true).unwrap();

            assert_eq!(emitter.len(), 2);
        }

        {
            emitter.emit(&mut TestStream, Some(3), true).unwrap();

            assert_eq!(emitter.len(), 0);
        }

        {
            emitter.emit(&mut TestStream, Some(3), true).unwrap();

            assert_eq!(emitter.len(), 0);
        }

        emitter.push((SystemTime::now(), vec![0x00, 0x01]));
        emitter.push((SystemTime::now(), vec![0x00, 0x02]));
        emitter.push((SystemTime::now(), vec![0x00, 0x03]));
        emitter.push((SystemTime::now(), vec![0x00, 0x04]));
        emitter.push((SystemTime::now(), vec![0x00, 0x05]));

        {
            assert_eq!(emitter.len(), 5);
        }

        {
            emitter.emit(&mut TestStream, None, true).unwrap();
            assert_eq!(emitter.len(), 0);
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
        let mut emitter = Emitter::new("x".to_string());

        emitter.push((SystemTime::now(), vec![0x00, 0x01]));
        emitter.push((SystemTime::now(), vec![0x00, 0x02]));
        emitter.push((SystemTime::now(), vec![0x00, 0x03]));
        emitter.push((SystemTime::now(), vec![0x00, 0x04]));
        emitter.push((SystemTime::now(), vec![0x00, 0x05]));

        assert!(emitter.emit(&mut TestErrStream, Some(3), false).is_err());
        assert_eq!(emitter.len(), 2);
    }

    #[test]
    fn emit_not_consume_queue_with_error_stream_if_does_recover_is_true() {
        let mut emitter = Emitter::new("x".to_string());

        emitter.push((SystemTime::now(), vec![0x00, 0x01]));
        emitter.push((SystemTime::now(), vec![0x00, 0x02]));
        emitter.push((SystemTime::now(), vec![0x00, 0x03]));
        emitter.push((SystemTime::now(), vec![0x00, 0x04]));
        emitter.push((SystemTime::now(), vec![0x00, 0x05]));

        assert!(emitter.emit(&mut TestErrStream, Some(3), true).is_err());

        assert_eq!(emitter.len(), 5);
    }

    #[test]
    fn emit_valid_chunks() {
        struct Mock {
            acc: Vec<(Vec<u8>, String)>,
        }

        impl Mock {
            pub fn new() -> Mock {
                Mock { acc: Vec::new() }
            }
        }

        impl WriteRead for Mock {
            fn write_and_read(&mut self, buf: &[u8], chunk: &str) -> Result<(), StreamError> {
                let args = (buf.to_vec(), chunk.to_string());
                self.acc.push(args);
                Ok(())
            }
        }

        let mut emitter = Emitter::new("x".to_string());

        for i in 1..1000 {
            emitter.push((SystemTime::now(), vec![0x00, (i as u8)]));
        }

        let rw = &mut Mock::new();
        emitter.emit(rw, Some(2), true).unwrap();

        let acc = &rw.acc;
        let chunks = acc
            .iter()
            .map(|(_, v)| general_purpose::STANDARD.decode(v).unwrap())
            .collect::<Vec<Vec<u8>>>();

        for chunk in chunks.iter() {
            let _ = Uuid::parse_str(&String::from_utf8(chunk.clone()).unwrap()).unwrap();
        }
    }
}
