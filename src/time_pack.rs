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
