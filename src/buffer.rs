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
