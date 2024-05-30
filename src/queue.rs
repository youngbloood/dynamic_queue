use anyhow::Result;
use parking_lot::Mutex;

#[async_trait::async_trait]
pub trait Queue: Send + Sync {
    type Item;
    async fn push(&self, _: Self::Item) -> Result<()>;
    async fn pop(&self) -> Option<Self::Item>;
}

pub struct DefaultQueue<T> {
    queue: Mutex<Vec<T>>,
}

impl<T> DefaultQueue<T> {
    pub fn new(size: usize) -> Self {
        DefaultQueue {
            queue: Mutex::new(Vec::with_capacity(size)),
        }
    }

    pub fn len(&self) -> usize {
        let lock = self.queue.lock();
        lock.len()
    }
}

#[async_trait::async_trait]
impl<T> Queue for DefaultQueue<T>
where
    T: Send + Sync,
{
    type Item = T;

    async fn push(&self, t: T) -> Result<()> {
        let mut lock = self.queue.lock();
        lock.push(t);
        Ok(())
    }

    async fn pop(&self) -> Option<T> {
        let mut lock = self.queue.lock();
        lock.pop()
    }
}

#[cfg(test)]
mod tests {
    use super::{DefaultQueue, Queue};

    #[test]
    fn test_defaultqueue_push() {
        let dq = DefaultQueue::new(4);
        for i in 0..10 {
            let _ = dq.push(i);
        }
        assert_eq!(dq.len(), 10);

        for i in 10..20 {
            let _ = dq.push(i);
        }
        assert_eq!(dq.len(), 20);
    }

    #[tokio::test]
    async fn test_defaultqueue_pop_null() {
        let dq = DefaultQueue::<u64>::new(4);
        for _ in 0..10 {
            let _ = dq.pop();
            assert_eq!(dq.pop().await.is_none(), true);
        }
    }

    #[tokio::test]
    async fn test_defaultqueue_pop() {
        let dq = DefaultQueue::<u64>::new(4);
        assert_eq!(dq.push(1).await.is_ok(), true);
        assert_eq!(dq.push(2).await.is_ok(), true);
        assert_eq!(dq.push(3).await.is_ok(), true);
        assert_eq!(dq.push(4).await.is_ok(), true);
        assert_eq!(dq.push(5).await.is_ok(), true);
        assert_eq!(dq.len(), 5);

        for _ in 0..5 {
            assert_eq!(dq.pop().await.is_some(), true);
        }

        for _ in 0..10 {
            assert_eq!(dq.pop().await.is_none(), true);
        }
    }
}
