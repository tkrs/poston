use std::collections::VecDeque;

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

#[cfg(test)]
mod test {
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
