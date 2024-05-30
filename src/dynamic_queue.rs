use crate::queue::Queue;
use anyhow::Result;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use tokio::sync::Semaphore;

pub struct DynamicQueue<T> {
    /// capacity
    cap: AtomicU64,

    /// available semaphore number
    available_semaphore_num: AtomicU64,

    /// tokio::sync::Semaphore
    semaphore: Semaphore,

    //// queue of push and pop
    queue: Box<dyn Queue<Item = T> + 'static>,
}

impl<T> DynamicQueue<T>
where
    T: Send + Sync,
{
    pub fn new(size: usize, q: impl Queue<Item = T> + 'static) -> Self {
        DynamicQueue {
            cap: AtomicU64::new(size as _),
            available_semaphore_num: AtomicU64::new(size as _),
            semaphore: Semaphore::new(size),
            queue: Box::new(q),
        }
    }

    fn resize(&self, size: usize) -> Result<()> {
        if self.cap.load(SeqCst) == size as u64 {
            return Ok(());
        }

        if size as u64 > self.cap.load(SeqCst) {
            let diff = size - self.cap.load(SeqCst) as usize;
            self.available_semaphore_num.fetch_add(diff as _, SeqCst);
            self.semaphore.add_permits(diff);
        } else {
            let diff = self.cap.load(SeqCst) as usize - size;
            self.available_semaphore_num.fetch_sub(diff as _, SeqCst);
            self.semaphore.forget_permits(diff);
        }
        self.cap.store(size as _, SeqCst);
        Ok(())
    }

    async fn push(&self, t: T) -> Result<()> {
        let permit = self.semaphore.acquire().await?;
        permit.forget();
        self.available_semaphore_num.fetch_sub(1, SeqCst);
        self.queue.push(t).await?;
        Ok(())
    }

    async fn pop(&self) -> Option<T> {
        if let Some(msg) = self.queue.pop().await {
            if self.available_semaphore_num.load(SeqCst) < self.cap.load(SeqCst) {
                self.semaphore.add_permits(1);
                self.available_semaphore_num.fetch_add(1, SeqCst);
            }

            return Some(msg);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use tokio::select;
    use tokio::time::interval;

    use super::DynamicQueue;
    use crate::queue::DefaultQueue;

    const TIMEOUT: u64 = 5;

    #[tokio::test]
    async fn test_dynamic_queue_push() {
        let dqueue = DynamicQueue::new(10, DefaultQueue::<u64>::new(5));
        for i in 0..10 {
            let _ = dqueue.push(i).await;
        }

        let mut ticker = interval(Duration::from_secs(TIMEOUT));
        ticker.tick().await;
        select! {
            _ = ticker.tick() => {
                eprintln!("push should timeout");
            }

            _ = dqueue.push(11) => {
               unreachable!();
            }
        }
    }

    #[tokio::test]
    async fn test_dynamic_queue_pop_null() {
        let dqueue = DynamicQueue::new(10, DefaultQueue::<u64>::new(5));

        for _ in 0..10 {
            assert_eq!(dqueue.pop().await.is_none(), true)
        }
    }

    #[tokio::test]
    async fn test_dynamic_queue_pop_not_null() {
        let dqueue = DynamicQueue::new(10, DefaultQueue::<u64>::new(5));

        for i in 0..10 {
            assert_eq!(dqueue.push(i).await.is_ok(), true)
        }
        for _ in 0..10 {
            assert_eq!(dqueue.pop().await.is_some(), true)
        }
    }

    #[tokio::test]
    async fn test_dynamic_queue_push_resize_increase() {
        let dqueue = DynamicQueue::new(10, DefaultQueue::<u64>::new(5));

        for i in 0..10 {
            assert_eq!(dqueue.push(i).await.is_ok(), true)
        }

        // block
        let mut ticker = interval(Duration::from_secs(TIMEOUT));
        ticker.tick().await;
        select! {
            _ = ticker.tick() => {
                eprintln!("push should timeout");
            }

            _ = dqueue.push(11) => {
               unreachable!();
            }
        }

        // resize increase
        let _ = dqueue.resize(15);
        for i in 10..15 {
            assert_eq!(dqueue.push(i).await.is_ok(), true)
        }
        select! {
            _ = ticker.tick() => {
                eprintln!("push should timeout again");
            }

            _ = dqueue.push(11) => {
               unreachable!();
            }
        }
    }

    #[tokio::test]
    async fn test_dynamic_queue_push_resize_decrease() {
        let dqueue = DynamicQueue::new(10, DefaultQueue::<u64>::new(5));

        for i in 0..10 {
            assert_eq!(dqueue.push(i).await.is_ok(), true)
        }

        // block
        let mut ticker = interval(Duration::from_secs(TIMEOUT));
        ticker.tick().await;
        select! {
            _ = ticker.tick() => {
                eprintln!("push should timeout");
            }

            _ = dqueue.push(11) => {
               unreachable!();
            }
        }

        // resize decrease
        let _ = dqueue.resize(5);
        select! {
            _ = ticker.tick() => {
                eprintln!("push should timeout again");
            }

            _ = dqueue.push(11) => {
               unreachable!();
            }
        }

        for _ in 0..10 {
            assert_eq!(dqueue.pop().await.is_some(), true)
        }

        for _ in 0..10 {
            assert_eq!(dqueue.pop().await.is_none(), true)
        }
    }

    #[tokio::test]
    async fn test_dynamic_queue_push_resize_increase_and_decrease() {
        let dqueue = DynamicQueue::new(10, DefaultQueue::<u64>::new(5));

        for i in 0..10 {
            assert_eq!(dqueue.push(i).await.is_ok(), true)
        }

        // block
        let mut ticker = interval(Duration::from_secs(TIMEOUT));
        ticker.tick().await;
        select! {
            _ = ticker.tick() => {
                eprintln!("push should timeout");
            }

            _ = dqueue.push(11) => {
               unreachable!();
            }
        }

        // resize increase
        let _ = dqueue.resize(15);
        for i in 10..15 {
            assert_eq!(dqueue.push(i).await.is_ok(), true)
        }
        select! {
            _ = ticker.tick() => {
                eprintln!("push should timeout again");
            }

            _ = dqueue.push(11) => {
               unreachable!();
            }
        }

        // resize increase
        let _ = dqueue.resize(5);
        select! {
            _ = ticker.tick() => {
                eprintln!("push should timeout again again");
            }

            _ = dqueue.push(11) => {
               unreachable!();
            }
        }

        for _ in 0..15 {
            assert_eq!(dqueue.pop().await.is_some(), true)
        }

        for _ in 0..10 {
            assert_eq!(dqueue.pop().await.is_none(), true)
        }
    }

    #[tokio::test]
    async fn test_dynamic_queue_push_resize_decrease_and_increase() {
        let dqueue = DynamicQueue::new(10, DefaultQueue::<u64>::new(5));

        for i in 0..10 {
            assert_eq!(dqueue.push(i).await.is_ok(), true)
        }

        // block
        let mut ticker = interval(Duration::from_secs(TIMEOUT));
        ticker.tick().await;
        select! {
            _ = ticker.tick() => {
                eprintln!("push should timeout");
            }

            _ = dqueue.push(11) => {
               unreachable!();
            }
        }

        // resize increase
        let _ = dqueue.resize(5);
        select! {
            _ = ticker.tick() => {
                eprintln!("push should timeout again");
            }

            _ = dqueue.push(11) => {
               unreachable!();
            }
        }

        // resize increase
        let _ = dqueue.resize(15);
        for i in 10..20 {
            assert_eq!(dqueue.push(i).await.is_ok(), true);
        }
        select! {
            _ = ticker.tick() => {
                eprintln!("push should timeout again again");
            }

            _ = dqueue.push(11) => {
                unreachable!();
            }
        }

        for _ in 0..20 {
            assert_eq!(dqueue.pop().await.is_some(), true)
        }

        for _ in 0..10 {
            assert_eq!(dqueue.pop().await.is_none(), true)
        }
    }
}
