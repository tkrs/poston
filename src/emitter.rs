use crate::buffer::{BufferError, Record};
use crate::connect::*;
use base64::{engine::general_purpose, Engine as _};
use std::time::SystemTime;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
pub struct Emitter {
    tag: String,
    chunk_id: String,
    entries: Vec<(SystemTime, Vec<u8>)>,
}

impl Emitter {
    pub fn new(tag: String, chunk_id: Uuid, entries: Vec<(SystemTime, Vec<u8>)>) -> Self {
        let chunk_id = general_purpose::STANDARD.encode(chunk_id.to_string());
        Self {
            tag,
            chunk_id,
            entries,
        }
    }

    pub fn tag(&self) -> &str {
        &self.tag
    }

    pub fn chunk_id(&self) -> &str {
        &self.chunk_id
    }

    #[cfg(test)]
    pub fn entries(&self) -> &Vec<(SystemTime, Vec<u8>)> {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn emit<RW: WriteRead>(&self, rw: &mut RW) -> Result<(), EmitterError> {
        debug!(
            "Emitting chunk with tag: {}, id: {}, entries: {}",
            self.tag,
            self.chunk_id,
            self.entries.len()
        );

        let rec = Record::new(&self.tag, self.entries.as_slice(), &self.chunk_id);
        let buf = rec.pack().map_err(EmitterError::Buffer)?;
        rw.write_and_read(&buf, &self.chunk_id)
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

    struct TestErrStream;

    impl WriteRead for TestErrStream {
        fn write_and_read(&mut self, _buf: &[u8], _chunk: &str) -> Result<(), StreamError> {
            Err(StreamError::AckUmatched("a".to_string(), "b".to_string()))
        }
    }

    #[test]
    fn emit_return_error_when_flusher_failed() {
        let uuid = Uuid::new_v4();
        let messages = vec![
            (SystemTime::now(), vec![0x00, 0x01]),
            (SystemTime::now(), vec![0x00, 0x02]),
            (SystemTime::now(), vec![0x00, 0x03]),
            (SystemTime::now(), vec![0x00, 0x04]),
            (SystemTime::now(), vec![0x00, 0x05]),
        ];

        let emitter = Emitter::new("x".to_string(), uuid, messages);

        assert!(emitter.emit(&mut TestErrStream).is_err());
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

        let mut messages = Vec::new();
        for i in 1..10 {
            messages.push((SystemTime::now(), vec![0x00, (i as u8)]));
        }
        let uuid = Uuid::new_v4();
        let emitter = Emitter::new("x".to_string(), uuid, messages);

        let rw = &mut Mock::new();
        emitter.emit(rw).unwrap();

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
