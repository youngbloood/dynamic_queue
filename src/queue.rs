use anyhow::Result;
use parking_lot::RwLock;

#[async_trait::async_trait]
pub trait Queue: Send + Sync {
    type Item;

    /// push Item into Queue.
    async fn push(&self, _: Self::Item) -> Result<()>;

    /// pop Item from Queue.
    async fn pop(&self) -> Option<Self::Item>;
}

pub struct DefaultQueue<T> {
    queue: RwLock<Vec<T>>,
}

impl<T> DefaultQueue<T> {
    pub fn new(size: usize) -> Self {
        DefaultQueue {
            queue: RwLock::new(Vec::with_capacity(size)),
        }
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        let lock = self.queue.read();
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
        let mut lock = self.queue.write();
        lock.push(t);
        // let mut iter = lock.iter();
        // while let Some(n) = iter.next() {
        //     println!("t = {n:?}");
        // }
        Ok(())
    }

    async fn pop(&self) -> Option<T> {
        let mut lock = self.queue.write();
        lock.pop()
    }
}

#[cfg(test)]
mod tests {

    use super::{DefaultQueue, Queue};

    #[tokio::test]
    async fn test_defaultqueue_push() {
        let dq = DefaultQueue::new(4);
        for i in 0..10 {
            let _ = dq.push(i).await;
        }
        assert_eq!(dq.len(), 10);

        for i in 10..20 {
            let _ = dq.push(i).await;
        }
        assert_eq!(dq.len(), 20);
    }

    #[tokio::test]
    async fn test_defaultqueue_pop_null() {
        let dq = DefaultQueue::<u64>::new(4);
        for _ in 0..10 {
            let _ = dq.pop().await;
            assert!(dq.pop().await.is_none());
        }
    }

    #[tokio::test]
    async fn test_defaultqueue_pop() {
        let dq = DefaultQueue::<u64>::new(4);
        assert!(dq.push(1).await.is_ok());
        assert!(dq.push(2).await.is_ok());
        assert!(dq.push(3).await.is_ok());
        assert!(dq.push(4).await.is_ok());
        assert!(dq.push(5).await.is_ok());
        assert_eq!(dq.len(), 5);

        for _ in 0..5 {
            assert!(dq.pop().await.is_some());
        }

        for _ in 0..10 {
            assert!(dq.pop().await.is_none());
        }
    }
}
