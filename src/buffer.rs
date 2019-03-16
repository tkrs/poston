use crate::error::Error;
use crate::rmps::encode::StructMapWriter;
use crate::rmps::{Deserializer, Serializer};
use crate::time_pack::TimePack;
use rmp::encode;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::error::Error as StdError;
use std::time::SystemTime;

pub trait Take<T> {
    fn take(&mut self, buf: &mut Vec<T>);
}

impl<T> Take<T> for VecDeque<T> {
    fn take(&mut self, buf: &mut Vec<T>) {
        let sz = buf.capacity();
        for _ in 0..sz {
            if let Some(e) = self.remove(0) {
                buf.push(e);
                continue;
            }
            break;
        }
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

pub fn pack_record<'a>(
    buf: &mut Vec<u8>,
    tag: &'a str,
    entries: Vec<(SystemTime, Vec<u8>)>,
    chunk: &'a str,
) -> Result<(), Error> {
    buf.push(0x93);
    encode::write_str(buf, tag).map_err(|e| Error::DeriveError(e.description().to_string()))?;
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
    let options = Some(Options {
        chunk: chunk.to_string(),
    });
    options
        .serialize(&mut Serializer::with(buf, StructMapWriter))
        .map_err(|e| Error::DeriveError(e.description().to_string()))
}

pub fn unpack_response(resp_buf: &[u8], size: usize) -> Result<AckReply, Error> {
    let mut de = Deserializer::new(&resp_buf[0..size]);
    Deserialize::deserialize(&mut de).map_err(|e| Error::DeriveError(e.description().to_string()))
}

#[cfg(test)]
mod test_take {
    use super::Take;
    use std::collections::VecDeque;

    #[test]
    fn it_should_move_elements_to_passed_buffer() {
        let mut queue = VecDeque::new();
        for i in 0..5 {
            queue.push_back(i);
        }
        let mut buffer = Vec::with_capacity(3);

        queue.take(&mut buffer);

        assert_eq!(buffer, vec![0, 1, 2]);
        assert_eq!(queue.len(), 2);

        let mut buffer = Vec::with_capacity(3);

        queue.take(&mut buffer);

        assert_eq!(buffer, vec![3, 4]);
        assert_eq!(queue.len(), 0);
    }
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
            pack_record(&mut buf, t, es, c).unwrap();
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
        for (i, e) in vec![0x81u8, 0xa3, 0x61, 0x63, 0x6b, 0xa4, 0x61, 0x62, 0x63, 0x3d]
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
