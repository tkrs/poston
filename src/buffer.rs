use crate::rmps::decode as rdecode;
use crate::rmps::encode as rencode;
use crate::rmps::Deserializer;
use crate::time_pack::TimePack;
use rmp::encode;
use serde::Deserialize;
use serde::Serialize;
use std::time::SystemTime;
use thiserror::Error;

pub trait Buffer<T> {
    fn pack(&self) -> Result<Vec<u8>, BufferError>;
}

impl<T: Serialize> Buffer<T> for T {
    fn pack(&self) -> Result<Vec<u8>, BufferError> {
        let mut buf = Vec::new();
        rencode::write_named(&mut buf, self).map_err(BufferError::Pack)?;
        Ok(buf)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct Options {
    chunk: String,
}

#[derive(Debug, PartialEq, Deserialize)]
pub struct AckReply {
    pub ack: String,
}

impl TryFrom<&[u8]> for AckReply {
    type Error = BufferError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        unpack_response(value, value.len())
    }
}

pub struct Record<'a> {
    tag: &'a str,
    entries: &'a [(SystemTime, Vec<u8>)],
    chunk: &'a str,
}

impl<'a> Record<'a> {
    pub fn new(tag: &'a str, entries: &'a [(SystemTime, Vec<u8>)], chunk: &'a str) -> Self {
        Self {
            tag,
            entries,
            chunk,
        }
    }

    pub fn pack(&self) -> Result<Vec<u8>, BufferError> {
        let mut buf = Vec::new();
        pack_record(&mut buf, self.tag, self.entries, self.chunk)?;
        Ok(buf)
    }
}

fn pack_record<'a>(
    buf: &mut Vec<u8>,
    tag: &'a str,
    entries: &'a [(SystemTime, Vec<u8>)],
    chunk: &'a str,
) -> Result<(), BufferError> {
    buf.push(0x93);
    encode::write_str(buf, tag)
        .map_err(|e| BufferError::Pack(rencode::Error::InvalidValueWrite(e)))?;
    encode::write_array_len(buf, entries.len() as u32)
        .map_err(|e| BufferError::Pack(rencode::Error::InvalidValueWrite(e)))?;
    for (t, entry) in entries {
        buf.push(0x92);
        t.write_time(buf)
            .map_err(|e| BufferError::Pack(rencode::Error::InvalidValueWrite(e)))?;
        buf.extend(entry.iter());
    }
    let options = Options {
        chunk: chunk.to_string(),
    };

    rencode::write_named(buf, &options).map_err(BufferError::Pack)
}

fn unpack_response(resp_buf: &[u8], size: usize) -> Result<AckReply, BufferError> {
    let mut de = Deserializer::new(&resp_buf[0..size]);
    Deserialize::deserialize(&mut de).map_err(BufferError::Unpack)
}

#[derive(Error, Debug)]
pub enum BufferError {
    #[error("pack failed")]
    Pack(#[from] rencode::Error),
    #[error("unpack failed")]
    Unpack(#[from] rdecode::Error),
}

#[cfg(test)]
mod test_pack_record {
    use super::pack_record;
    use std::ops::Add;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn it_should_make_forward_mode_buffer() {
        let matrix = vec![
            (
                (
                    "a",
                    vec![(
                        UNIX_EPOCH,
                        vec![
                            0x82, 0xa7, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x63, 0x74, 0xc3, 0xa6, 0x73,
                            0x63, 0x68, 0x65, 0x6d, 0x61, 0x00,
                        ],
                    )],
                    "f0c=",
                ),
                vec![
                    0x93, 0xa1, 0x61, 0x91, 0x92, 0xd7, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x82, 0xa7, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x63, 0x74, 0xc3, 0xa6,
                    0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x00, 0x81, 0xa5, 0x63, 0x68, 0x75, 0x6e,
                    0x6b, 0xa4, 0x66, 0x30, 0x63, 0x3d,
                ],
            ),
            (
                (
                    "b",
                    vec![
                        (UNIX_EPOCH, vec![0x00]),
                        (UNIX_EPOCH.add(Duration::new(0, 100)), vec![0x01]),
                    ],
                    "f0c=",
                ),
                vec![
                    0x93, 0xa1, 0x62, 0x92, 0x92, 0xd7, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x92, 0xd7, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x64, 0x01, 0x81, 0xa5, 0x63, 0x68, 0x75, 0x6e, 0x6b, 0xa4, 0x66, 0x30, 0x63,
                    0x3d,
                ],
            ),
        ];

        for ((t, es, c), expected) in matrix {
            let mut buf = Vec::new();
            pack_record(&mut buf, t, es.as_slice(), c).unwrap();
            assert_eq!(buf, expected);
        }
    }
}

#[cfg(test)]
mod unpack_response {
    use super::{unpack_response, AckReply};

    #[test]
    fn it_should_unpack_as_ack_reply() {
        let mut resp_buf = [0u8; 64];
        for (i, e) in [0x81u8, 0xa3, 0x61, 0x63, 0x6b, 0xa4, 0x61, 0x62, 0x63, 0x3d]
            .iter()
            .enumerate()
        {
            resp_buf[i] = *e;
        }

        let r = unpack_response(&resp_buf, 10).unwrap();
        assert_eq!(
            r,
            AckReply {
                ack: "abc=".to_string()
            }
        )
    }
}
