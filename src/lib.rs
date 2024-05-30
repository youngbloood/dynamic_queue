use anyhow::Result;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;
use tokio::sync::Semaphore;

pub trait Queue<T>
where
    T: Send + Sync + 'static,
{
    // type Item;
    fn push(&mut self, _: T) -> Result<()>;
    fn pop(&mut self) -> Option<T>;
}

struct DefaultQueue<T> {
    queue: Vec<T>,
}

impl<T> DefaultQueue<T> {
    fn new(size: usize) -> Self {
        DefaultQueue {
            queue: Vec::with_capacity(size),
        }
    }
}

impl<T> Queue<T> for DefaultQueue<T>
where
    T: Send + Sync + 'static,
{
    // type Item = T;

    fn push(&mut self, t: T) -> Result<()> {
        self.queue.push(t);
        Ok(())
    }

    fn pop(&mut self) -> Option<T> {
        self.queue.pop()
    }
}

pub struct DynamicQueue<T> {
    cap: usize,
    available_semaphre_num: AtomicU64,
    semaphore: Semaphore,
    queue: Box<dyn Queue<T>>,
}

// impl<T> DynamicQueue<T> {

// }

impl<T> DynamicQueue<T>
where
    T: Send + Sync + 'static,
{
    pub fn new_default(size: usize) -> Self {
        DynamicQueue {
            cap: size,
            available_semaphre_num: AtomicU64::new(0),
            semaphore: Semaphore::new(size),
            queue: Box::new(DefaultQueue::new(size)),
        }
    }

    pub fn new(size: usize, q: Box<dyn Queue<T>>) -> Self {
        DynamicQueue {
            cap: size,
            available_semaphre_num: AtomicU64::new(0),
            semaphore: Semaphore::new(size),
            queue: q,
        }
    }

    async fn push(&mut self, t: T) -> Result<()> {
        let permit = self.semaphore.acquire().await?;
        permit.forget();
        self.queue.push(t);
        Ok(())
    }

    async fn pop(&mut self) -> Option<T> {
        if let Some(msg) = self.queue.pop() {
            if self.available_semaphre_num.load(SeqCst) < self.cap as u64 {
                self.semaphore.add_permits(1);
                self.available_semaphre_num.fetch_add(1, SeqCst);
            }

            return Some(msg);
        }
        None
    }

    async fn resize(&mut self, size: usize) -> Result<()> {
        if size == self.cap {
            return Ok(());
        }
        if size > self.cap {
            self.semaphore.add_permits(size - self.cap);
        } else {
            self.semaphore.forget_permits(self.cap - size);
        }
        self.cap = size;
        Ok(())
    }
}

// #[cfg(test)]
// mod tests {
//     use crate::Queue;

//     use super::DynamicQueue;
//     use std::time::Duration;

//     #[tokio::test]
//     async fn test_memory_push() {
//         let mut mem = DynamicQueue::new_default(10);
//         for _ in 0..10 {
//             let _ = mem.push(1).await;
//         }
//     }

//     struct Dummy {}
//     impl<u64> Queue<u64> for Dummy {
//         fn push(&mut self, _: u64) -> anyhow::Result<()> {
//             todo!()
//         }

//         fn pop(&mut self) -> Option<u64> {
//             todo!()
//         }
//     }

//     #[tokio::test]
//     async fn test_memory_push_block() {
//         let mut mem = DynamicQueue::new(10);
//         for _ in 0..10 {
//             let _ = mem.push(Message::new()).await;
//         }
//         let mut ticker = interval(Duration::from_secs(10)).await;
//         select! {
//             _ = ticker.tick() => {
//                 eprintln!("push timeout");
//             }
//             _ = mem.push(Message::new()) => {
//                 panic!("should not push success");
//             }
//         }
//     }

//     #[tokio::test]
//     async fn test_memory_push_block_with_zero() {
//         let mut mem: DynamicQueue = DynamicQueue::new(0);

//         let mut ticker = interval(Duration::from_secs(10)).await;
//         select! {
//             _ = ticker.tick() => {
//                 eprintln!("push timeout");
//             }
//             _ = mem.push(Message::new()) => {
//                 panic!("should not push success");
//             }
//         }
//     }

//     #[tokio::test]
//     async fn test_memory_pop() {
//         let mut mem = DynamicQueue::new(10);
//         for _ in 0..10 {
//             let _ = mem.push(Message::new()).await;
//         }

//         for _ in 0..10 {
//             assert_eq!(mem.pop().await.is_some(), true);
//         }
//     }

//     #[tokio::test]
//     async fn test_memory_pop_none() {
//         let mut mem = DynamicQueue::new(10);
//         for _ in 0..10 {
//             let _ = mem.push(Message::new()).await;
//         }

//         for _ in 0..10 {
//             assert_eq!(mem.pop().await.is_some(), true);
//         }

//         for _ in 0..10 {
//             assert_eq!(mem.pop().await.is_some(), false);
//         }
//     }

//     #[tokio::test]
//     async fn test_memory_push_and_resize() {
//         let mut mem = DynamicQueue::new(0);

//         let mut ticker = interval(Duration::from_secs(10)).await;
//         select! {
//             _ = ticker.tick() => {
//                 eprintln!("push timeout");
//             }
//             _ = mem.push(Message::new()) => {
//                 panic!("should not push success");
//             }
//         }

//         let _ = mem.resize(10).await;
//         for _ in 0..10 {
//             let _ = mem.push(Message::new()).await;
//         }

//         let _ = mem.resize(20).await;
//         for _ in 0..10 {
//             let _ = mem.push(Message::new()).await;
//         }

//         let _ = mem.resize(10).await;
//         select! {
//             _ = ticker.tick() => {
//                 eprintln!("push timeout2");
//             }
//             _ = mem.push(Message::new()) => {
//                 panic!("should not push success");
//             }
//         }
//     }
// }
