use crate::queue::Queue;
use anyhow::Result;
use std::{
    sync::atomic::{AtomicU64, Ordering::SeqCst},
    usize,
};
use tokio::sync::Semaphore;

/// [`FlowControl`]  control the concurrency.
struct FlowControl {
    /// capacity
    cap: AtomicU64,

    /// available semaphore number
    available_semaphore_num: AtomicU64,

    /// tokio::sync::Semaphore
    semaphore: Semaphore,
}

impl FlowControl {
    fn new(size: usize) -> Self {
        FlowControl {
            cap: AtomicU64::new(size as _),
            available_semaphore_num: AtomicU64::new(size as _),
            semaphore: Semaphore::new(size),
        }
    }

    pub fn resize(&self, size: usize) {
        if self.cap.load(SeqCst) == size as u64 {
            return;
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
    }

    /// grant n tokens from [`FlowControl`]
    ///
    /// it will be block when the available tokens less than n.
    pub async fn grant(&self, mut n: usize) {
        while n != 0 {
            let permit = self.semaphore.acquire().await.unwrap();
            permit.forget();
            self.available_semaphore_num.fetch_sub(1, SeqCst);
            n -= 1;
        }
    }

    /// revert n tokens to [`FlowControl`]
    ///
    /// it can't be over the cap
    pub async fn revert(&self, mut n: usize) {
        while n != 0 {
            n -= 1;
            if self.available_semaphore_num.load(SeqCst) < self.cap.load(SeqCst) {
                self.semaphore.add_permits(1);
                self.available_semaphore_num.fetch_add(1, SeqCst);
            }
        }
    }
}

/// [`DynamicQueue`] is dynamic size buffer queue.
pub struct DynamicQueue<T> {
    ctrl: FlowControl,

    //// queue of push and pop
    queue: Box<dyn Queue<Item = T> + 'static>,
}

impl<T> DynamicQueue<T>
where
    T: Send + Sync,
{
    pub fn new(size: usize, q: impl Queue<Item = T> + 'static) -> Self {
        DynamicQueue {
            ctrl: FlowControl::new(size),
            queue: Box::new(q),
        }
    }

    pub fn resize(&self, size: usize) {
        self.ctrl.resize(size);
    }

    pub async fn push(&self, t: T) -> Result<()> {
        self.ctrl.grant(1).await;
        self.queue.push(t).await?;
        Ok(())
    }

    pub async fn pop(&self) -> Option<T> {
        if let Some(msg) = self.queue.pop().await {
            self.ctrl.revert(1).await;
            return Some(msg);
        }
        None
    }
}

/// [`DynamicQueueRef`] is dynamic size buffer queue.
///
/// You can use the [`Queue`]'s or custome functions beyond the [`DynamicQueueRef`]
///
/// That will not influence the [`DynamicQueueRef`] control the Dynamic flow.
pub struct DynamicQueueRef<'a, T> {
    ctrl: FlowControl,

    //// queue of push and pop
    queue: &'a mut dyn Queue<Item = T>,
}

impl<'a, T> DynamicQueueRef<'a, T>
where
    T: Send + Sync,
{
    pub fn new(size: usize, q: &'a mut dyn Queue<Item = T>) -> Self {
        DynamicQueueRef {
            ctrl: FlowControl::new(size),
            queue: q,
        }
    }

    pub fn resize(&self, size: usize) {
        self.ctrl.resize(size);
    }

    pub async fn push(&self, t: T) -> Result<()> {
        self.ctrl.grant(1).await;
        self.queue.push(t).await?;
        Ok(())
    }

    pub async fn pop(&self) -> Option<T> {
        if let Some(msg) = self.queue.pop().await {
            self.ctrl.revert(1).await;
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
    use crate::DynamicQueueRef;

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
            assert!(dqueue.pop().await.is_none())
        }
    }

    #[tokio::test]
    async fn test_dynamic_queue_pop_not_null() {
        let dqueue = DynamicQueue::new(10, DefaultQueue::<u64>::new(5));

        for i in 0..10 {
            assert!(dqueue.push(i).await.is_ok())
        }
        for _ in 0..10 {
            assert!(dqueue.pop().await.is_some())
        }
    }

    #[tokio::test]
    async fn test_dynamic_queue_push_resize_increase() {
        let dqueue = DynamicQueue::new(10, DefaultQueue::<u64>::new(5));

        for i in 0..10 {
            assert!(dqueue.push(i).await.is_ok())
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
        dqueue.resize(15);
        for i in 10..15 {
            assert!(dqueue.push(i).await.is_ok())
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
            assert!(dqueue.push(i).await.is_ok())
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
        dqueue.resize(5);
        select! {
            _ = ticker.tick() => {
                eprintln!("push should timeout again");
            }

            _ = dqueue.push(11) => {
               unreachable!();
            }
        }

        for _ in 0..10 {
            assert!(dqueue.pop().await.is_some())
        }

        for _ in 0..10 {
            assert!(dqueue.pop().await.is_none())
        }
    }

    #[tokio::test]
    async fn test_dynamic_queue_push_resize_increase_and_decrease() {
        let dqueue = DynamicQueue::new(10, DefaultQueue::<u64>::new(5));

        for i in 0..10 {
            assert!(dqueue.push(i).await.is_ok())
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
        dqueue.resize(15);
        for i in 10..15 {
            assert!(dqueue.push(i).await.is_ok())
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
        dqueue.resize(5);
        select! {
            _ = ticker.tick() => {
                eprintln!("push should timeout again again");
            }

            _ = dqueue.push(11) => {
               unreachable!();
            }
        }

        for _ in 0..15 {
            assert!(dqueue.pop().await.is_some())
        }

        for _ in 0..10 {
            assert!(dqueue.pop().await.is_none())
        }
    }

    #[tokio::test]
    async fn test_dynamic_queue_push_resize_decrease_and_increase() {
        let dqueue = DynamicQueue::new(10, DefaultQueue::<u64>::new(5));

        for i in 0..10 {
            assert!(dqueue.push(i).await.is_ok())
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
        dqueue.resize(5);
        select! {
            _ = ticker.tick() => {
                eprintln!("push should timeout again");
            }

            _ = dqueue.push(11) => {
               unreachable!();
            }
        }

        // resize increase
        dqueue.resize(15);
        for i in 10..20 {
            assert!(dqueue.push(i).await.is_ok());
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
            assert!(dqueue.pop().await.is_some())
        }

        for _ in 0..10 {
            assert!(dqueue.pop().await.is_none())
        }
    }

    // DynamicQueueRef
    #[tokio::test]
    async fn test_dynamic_queue_ref_push() {
        let mut queue = DefaultQueue::<u64>::new(5);
        let dqueue = DynamicQueueRef::new(10, &mut queue);
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
    async fn test_dynamic_queue_ref_pop_null() {
        let mut queue = DefaultQueue::<u64>::new(5);
        let dqueue = DynamicQueueRef::new(10, &mut queue);

        for _ in 0..10 {
            assert!(dqueue.pop().await.is_none())
        }
    }

    #[tokio::test]
    async fn test_dynamic_queue_ref_pop_not_null() {
        let mut queue = DefaultQueue::<u64>::new(5);
        let dqueue = DynamicQueueRef::new(10, &mut queue);

        for i in 0..10 {
            assert!(dqueue.push(i).await.is_ok())
        }
        for _ in 0..10 {
            assert!(dqueue.pop().await.is_some())
        }
    }

    #[tokio::test]
    async fn test_dynamic_queue_ref_push_resize_increase() {
        let mut queue = DefaultQueue::<u64>::new(5);
        let dqueue = DynamicQueueRef::new(10, &mut queue);

        for i in 0..10 {
            assert!(dqueue.push(i).await.is_ok())
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
        dqueue.resize(15);
        for i in 10..15 {
            assert!(dqueue.push(i).await.is_ok())
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
    async fn test_dynamic_queue_ref_push_resize_decrease() {
        let mut queue = DefaultQueue::<u64>::new(5);
        let dqueue = DynamicQueueRef::new(10, &mut queue);

        for i in 0..10 {
            assert!(dqueue.push(i).await.is_ok())
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
        dqueue.resize(5);
        select! {
            _ = ticker.tick() => {
                eprintln!("push should timeout again");
            }

            _ = dqueue.push(11) => {
               unreachable!();
            }
        }

        for _ in 0..10 {
            assert!(dqueue.pop().await.is_some())
        }

        for _ in 0..10 {
            assert!(dqueue.pop().await.is_none())
        }
    }

    #[tokio::test]
    async fn test_dynamic_queue_ref_push_resize_increase_and_decrease() {
        let mut queue = DefaultQueue::<u64>::new(5);
        let dqueue = DynamicQueueRef::new(10, &mut queue);

        for i in 0..10 {
            assert!(dqueue.push(i).await.is_ok())
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
        dqueue.resize(15);
        for i in 10..15 {
            assert!(dqueue.push(i).await.is_ok())
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
        dqueue.resize(5);
        select! {
            _ = ticker.tick() => {
                eprintln!("push should timeout again again");
            }

            _ = dqueue.push(11) => {
               unreachable!();
            }
        }

        for _ in 0..15 {
            assert!(dqueue.pop().await.is_some())
        }

        for _ in 0..10 {
            assert!(dqueue.pop().await.is_none())
        }
    }

    #[tokio::test]
    async fn test_dynamic_queue_ref_push_resize_decrease_and_increase() {
        let mut queue = DefaultQueue::<u64>::new(5);
        let dqueue = DynamicQueueRef::new(10, &mut queue);

        for i in 0..10 {
            assert!(dqueue.push(i).await.is_ok())
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
        dqueue.resize(5);
        select! {
            _ = ticker.tick() => {
                eprintln!("push should timeout again");
            }

            _ = dqueue.push(11) => {
               unreachable!();
            }
        }

        // resize increase
        dqueue.resize(15);
        for i in 10..20 {
            assert!(dqueue.push(i).await.is_ok());
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
            assert!(dqueue.pop().await.is_some())
        }

        for _ in 0..10 {
            assert!(dqueue.pop().await.is_none())
        }
    }
}
