use crate::connect::*;
use crate::emitter::Emitter;
use std::collections::HashMap;
use std::time::SystemTime;

pub trait Queue {
    fn push(&mut self, tag: String, tm: SystemTime, msg: Vec<u8>);
    fn flush(&mut self, size: Option<usize>);
    fn len(&self) -> usize;
}

pub struct QueueHandler<S: WriteRead> {
    pub emitters: HashMap<String, Emitter>,
    pub flusher: S,
}

impl<S: WriteRead> Queue for QueueHandler<S> {
    fn push(&mut self, tag: String, tm: SystemTime, msg: Vec<u8>) {
        let emitter = self
            .emitters
            .entry(tag.clone())
            .or_insert_with(|| Emitter::new(tag));
        emitter.push((tm, msg));
    }

    fn flush(&mut self, size: Option<usize>) {
        for (tag, emitter) in self.emitters.iter() {
            if let Err(err) = emitter.emit(&mut self.flusher, size) {
                error!(
                    "Tag '{}' unexpected error occurred during emitting message, cause: '{:?}'",
                    tag, err
                );
            }
        }
    }

    fn len(&self) -> usize {
        self.emitters.values().map(|e| e.len()).sum()
    }
}

#[derive(PartialEq, Debug)]
pub enum HandleResult {
    Queued,
    Flushed,
    Terminated,
}
