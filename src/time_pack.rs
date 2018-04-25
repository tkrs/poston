use rmp::encode::ValueWriteError;
use std::time::{SystemTime, UNIX_EPOCH};

pub trait TimePack {
    fn write_time(&self, buf: &mut Vec<u8>) -> Result<(), ValueWriteError>;
}

impl TimePack for SystemTime {
    fn write_time(&self, buf: &mut Vec<u8>) -> Result<(), ValueWriteError> {
        let d = self.duration_since(UNIX_EPOCH).unwrap();
        buf.push(0xd7);
        buf.push(0x00);

        let epoch_secs = d.as_secs() as u32;
        let nanos = d.subsec_nanos();

        write_u32(buf, epoch_secs);
        write_u32(buf, nanos);

        Ok(())
    }
}

fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.push((v >> 24) as u8);
    buf.push((v >> 16) as u8);
    buf.push((v >> 8) as u8);
    buf.push(v as u8);
}

#[cfg(test)]
mod test {
    use super::TimePack;
    use std::ops::Add;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn it_should_pack_to_event_time_format() {
        let matrix = vec![
            (
                UNIX_EPOCH,
                vec![0xd7, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            ),
            (
                UNIX_EPOCH.add(Duration::new(1524650081, 1234567)),
                vec![0xd7, 0x00, 0x5a, 0xe0, 0x50, 0x61, 0x00, 0x12, 0xd6, 0x87],
            ),
        ];

        for (t, expected) in matrix {
            let mut buf = Vec::new();
            t.write_time(&mut buf).unwrap();
            assert_eq!(buf, expected);
        }
    }
}
