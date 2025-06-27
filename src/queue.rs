use itertools::Itertools;
use uuid::Uuid;

use crate::connect::*;
use crate::emitter::Emitter;
use std::cmp;
use std::collections::VecDeque;
use std::time::SystemTime;

pub trait Queue {
    fn push(&mut self, tag: String, tm: SystemTime, msg: Vec<u8>);
    fn flush(&mut self, size: Option<usize>);
    fn len(&self) -> usize;
}

pub struct QueueHandler<S: WriteRead> {
    messages: VecDeque<Message>,
    failed_emitters: VecDeque<Emitter>,
    flusher: S,
    recovery_settings: RecoverySettings,
}

#[derive(Debug, Clone)]
struct Message {
    tag: String,
    tm: SystemTime,
    raw: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct RecoverySettings {
    recovery_mode: RecoveryMode,
}
impl RecoverySettings {
    pub fn new(recovery_mode: RecoveryMode) -> Self {
        Self { recovery_mode }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RecoveryMode {
    Discard,
    Enqueue,
}

impl Default for RecoveryMode {
    fn default() -> Self {
        RecoveryMode::Discard
    }
}

impl<S: WriteRead> QueueHandler<S> {
    pub fn new(flusher: S, recovery_settings: RecoverySettings) -> Self {
        Self {
            messages: VecDeque::new(),
            failed_emitters: VecDeque::new(),
            flusher,
            recovery_settings,
        }
    }

    fn grouped_emitters(&mut self, size: Option<usize>) -> Vec<Emitter> {
        let mes_len = self.messages.len();
        let size = cmp::min(mes_len, size.unwrap_or(mes_len));
        let messages = self.messages.drain(0..size).collect::<Vec<_>>();

        messages
            .into_iter()
            .into_group_map_by(|m| m.tag.clone())
            .iter()
            .map(|(key, chunk)| {
                Emitter::new(
                    key.clone(),
                    Uuid::new_v4(),
                    chunk
                        .iter()
                        .map(|Message { tm, raw, .. }| (*tm, raw.clone()))
                        .collect(),
                )
            })
            .collect()
    }
}

impl<S: WriteRead> Queue for QueueHandler<S> {
    fn push(&mut self, tag: String, tm: SystemTime, msg: Vec<u8>) {
        self.messages.push_back(Message { tag, tm, raw: msg });
    }

    fn flush(&mut self, size: Option<usize>) {
        if self.messages.is_empty() {
            return;
        }

        let mut emitters = self.grouped_emitters(size);

        // If there are any failed emitters, we need to re-emit them
        if !self.failed_emitters.is_empty() {
            emitters.extend(self.failed_emitters.drain(..));
        }

        let recovery_mode = &self.recovery_settings.recovery_mode;

        for emitter in emitters.iter_mut() {
            if let Err(err) = emitter.emit(&mut self.flusher) {
                match recovery_mode {
                    RecoveryMode::Discard => {
                        error!(
                            "Tag '{}' error occurred during emitting messages; they will be discarded. chunk: {}, cause: '{:?}'",
                            emitter.tag(), emitter.chunk_id(), err
                        );
                    }
                    RecoveryMode::Enqueue => {
                        warn!(
                            "Tag '{}' error occurred during emitting messages; they will be retried on the next attempt, chunk: {}. cause: '{:?}'",
                            emitter.tag(), emitter.chunk_id(), err
                        );
                        self.failed_emitters.push_back(emitter.clone());
                    }
                }
            }
        }
    }

    fn len(&self) -> usize {
        self.messages.len() + self.failed_emitters.iter().map(|e| e.len()).sum::<usize>()
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
    fn test_grouped_emitters() {
        struct W;
        impl WriteRead for W {
            fn write_and_read(&mut self, _buf: &[u8], _chunk: &str) -> Result<(), StreamError> {
                Ok(())
            }
        }

        let mut queue = QueueHandler {
            messages: VecDeque::new(),
            failed_emitters: VecDeque::new(),
            flusher: W,
            recovery_settings: RecoverySettings::new(RecoveryMode::Discard),
        };

        let now = SystemTime::now();

        queue.push("a".to_string(), now, vec![0u8, 9u8]);
        queue.push("b".to_string(), now, vec![1u8, 8u8]);
        queue.push("a".to_string(), now, vec![2u8, 7u8]);
        queue.push("b".to_string(), now, vec![3u8, 6u8]);
        queue.push("c".to_string(), now, vec![4u8, 5u8]);

        assert_eq!(
            queue
                .grouped_emitters(Some(3))
                .iter()
                .map(|v| (v.tag().to_string(), v.entries().clone()))
                .sorted()
                .collect::<Vec<_>>(),
            vec![
                (
                    "a".to_string(),
                    vec![(now, vec![0u8, 9u8]), (now, vec![2u8, 7u8])]
                ),
                ("b".to_string(), vec![(now, vec![1u8, 8u8])]),
            ]
        );

        assert_eq!(
            queue
                .grouped_emitters(Some(3))
                .iter()
                .map(|v| (v.tag().to_string(), v.entries().clone()))
                .sorted()
                .collect::<Vec<_>>(),
            vec![
                ("b".to_string(), vec![(now, vec![3u8, 6u8])]),
                ("c".to_string(), vec![(now, vec![4u8, 5u8])]),
            ]
        );
    }

    #[test]
    fn test_push_flush() {
        struct W;
        impl WriteRead for W {
            fn write_and_read(&mut self, _buf: &[u8], _chunk: &str) -> Result<(), StreamError> {
                Ok(())
            }
        }

        let messages = VecDeque::new();
        let failed_emitters = VecDeque::new();
        let flusher = W;

        let mut queue = QueueHandler {
            messages,
            failed_emitters,
            flusher,
            recovery_settings: RecoverySettings::new(RecoveryMode::Enqueue),
        };

        let now = SystemTime::now();

        queue.push("a".to_string(), now, vec![0u8, 9u8]);
        queue.push("b".to_string(), now, vec![1u8, 8u8]);
        queue.push("a".to_string(), now, vec![2u8, 7u8]);
        queue.push("b".to_string(), now, vec![3u8, 6u8]);
        queue.push("c".to_string(), now, vec![4u8, 5u8]);

        assert_eq!(queue.len(), 5);

        queue.flush(Some(3));

        assert_eq!(queue.len(), 2);

        queue.flush(None);

        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn test_flush_failed() {
        struct W;
        impl WriteRead for W {
            fn write_and_read(&mut self, _buf: &[u8], _chunk: &str) -> Result<(), StreamError> {
                Err(StreamError::AckUmatched("x".into(), "y".into()))
            }
        }

        let messages = VecDeque::new();
        let failed_emitters = VecDeque::new();
        let flusher = W;

        let mut queue = QueueHandler {
            messages,
            failed_emitters,
            flusher,
            recovery_settings: RecoverySettings::new(RecoveryMode::Enqueue),
        };

        let now = SystemTime::now();

        queue.push("a".to_string(), now, vec![0u8, 9u8]);
        queue.push("b".to_string(), now, vec![1u8, 8u8]);
        queue.push("a".to_string(), now, vec![2u8, 7u8]);
        queue.push("b".to_string(), now, vec![3u8, 6u8]);
        queue.push("c".to_string(), now, vec![4u8, 5u8]);

        queue.flush(Some(3));
        assert_eq!(queue.len(), 5);
        assert_eq!(
            queue
                .failed_emitters
                .iter()
                .clone()
                .map(|e| (e.tag().to_string(), e.entries().clone()))
                .sorted()
                .collect::<Vec<_>>(),
            vec![
                (
                    "a".to_string(),
                    vec![(now, vec![0u8, 9u8]), (now, vec![2u8, 7u8])]
                ),
                ("b".to_string(), vec![(now, vec![1u8, 8u8])]),
            ]
        );

        queue.flush(None);
        assert!(queue.messages.is_empty());
        assert_eq!(queue.len(), 5);
        assert_eq!(
            queue
                .failed_emitters
                .iter()
                .clone()
                .map(|e| (e.tag().to_string(), e.entries().clone()))
                .sorted()
                .collect::<Vec<_>>(),
            vec![
                (
                    "a".to_string(),
                    vec![(now, vec![0u8, 9u8]), (now, vec![2u8, 7u8])]
                ),
                ("b".to_string(), vec![(now, vec![1u8, 8u8])]),
                ("b".to_string(), vec![(now, vec![3u8, 6u8])]),
                ("c".to_string(), vec![(now, vec![4u8, 5u8])]),
            ]
        );
    }
}
