use base64;
use buffer::Take;
use error::Error;
use rmp::encode;
use rmps::encode::StructMapWriter;
use rmps::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::error::Error as StdError;
use std::io::Read;
use std::time::SystemTime;
use stream::ReconnectableWriter;
use time_pack::TimePack;
use uuid::Uuid;

pub struct Emitter {
    tag: String,
    queue: RefCell<VecDeque<(SystemTime, Vec<u8>)>>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct Options {
    chunk: String,
}

#[derive(Debug, Deserialize)]
struct AckReply {
    ack: String,
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

    fn make_buffer(
        &self,
        buf: &mut Vec<u8>,
        sz: Option<usize>,
        tag: String,
        chunk: String,
    ) -> Result<(), Error> {
        let mut queue = self.queue.borrow_mut();
        if queue.is_empty() {
            return Ok(());
        }
        let sz = sz.unwrap_or_else(|| queue.len());
        let mut entries = Vec::with_capacity(sz);
        queue.take(&mut entries);
        buf.push(0x93);
        encode::write_str(buf, tag.as_str())
            .map_err(|e| Error::DeriveError(e.description().to_string()))?;
        encode::write_array_len(buf, entries.len() as u32)
            .map_err(|e| Error::DeriveError(e.description().to_string()))?;
        for (t, entry) in entries {
            buf.push(0x92);
            t.write_time(buf)
                .map_err(|e| Error::DeriveError(e.description().to_string()))?;
            for elem in entry {
                buf.push(elem);
            }
        }
        let options = Some(Options { chunk });
        options
            .serialize(&mut Serializer::with(buf, StructMapWriter))
            .map_err(|e| Error::DeriveError(e.description().to_string()))
    }

    pub fn read_ack<R>(&self, r: &mut R) -> Result<String, Error>
    where
        R: Read,
    {
        // TODO: Do it need retry the read?
        let mut resp_buf = [0u8; 64];
        let sz = r.read(&mut resp_buf).map_err(Error::NetworkError)?;
        let mut de = Deserializer::new(&resp_buf[0..sz]);
        let ret: AckReply = Deserialize::deserialize(&mut de)
            .map_err(|e| Error::DeriveError(e.description().to_string()))?;
        Ok(ret.ack)
    }

    pub fn emit<'a, RW>(&self, rw: &mut RW, size: Option<usize>) -> Result<(), Error>
    where
        RW: ReconnectableWriter + Read,
    {
        let chunk = base64::encode(&Uuid::new_v4().to_string());
        let mut buf = Vec::new();
        self.make_buffer(&mut buf, size, self.tag.clone(), chunk.clone())?;

        rw.write(buf).map_err(Error::NetworkError)?;
        let ack = self.read_ack(rw)?;
        if ack == chunk {
            Ok(())
        } else {
            Err(Error::AckUmatchedError)
        }
    }
}
