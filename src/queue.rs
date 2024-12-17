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

impl<S: WriteRead> QueueHandler<S> {
    #[cfg(test)]
    fn emitters(&self) -> &HashMap<String, Emitter> {
        &self.emitters
    }
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
        self.emitters.values().map(Emitter::len).sum()
    }
}

#[derive(PartialEq, Debug)]
pub enum HandleResult {
    Queued,
    Flushed,
    Terminated,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_flush() {
        struct W;
        impl WriteRead for W {
            fn write_and_read(&mut self, _buf: &[u8], _chunk: &str) -> Result<(), StreamError> {
                Ok(())
            }
        }
        let emitters = HashMap::new();
        let flusher = W;

        let mut queue = QueueHandler { emitters, flusher };

        assert!(queue.emitters().is_empty());

        let now = SystemTime::now();

        queue.push("a".to_string(), now, vec![0u8, 9u8]);
        queue.push("b".to_string(), now, vec![1u8, 8u8]);
        queue.push("a".to_string(), now, vec![2u8, 7u8]);
        queue.push("b".to_string(), now, vec![3u8, 6u8]);
        queue.push("c".to_string(), now, vec![4u8, 5u8]);

        let expected = Emitter::new("a".to_string());
        expected.push((now, vec![0u8, 9u8]));
        expected.push((now, vec![2u8, 7u8]));
        assert_eq!(queue.emitters().get("a").unwrap(), &expected);

        let expected = Emitter::new("b".to_string());
        expected.push((now, vec![1u8, 8u8]));
        expected.push((now, vec![3u8, 6u8]));
        assert_eq!(queue.emitters().get("b").unwrap(), &expected);

        let expected = Emitter::new("c".to_string());
        expected.push((now, vec![4u8, 5u8]));
        assert_eq!(queue.emitters().get("c").unwrap(), &expected);

        assert_eq!(queue.len(), 5);

        queue.flush(Some(1));

        assert_eq!(queue.len(), 2);

        queue.flush(None);

        assert_eq!(queue.len(), 0);
    }
}
