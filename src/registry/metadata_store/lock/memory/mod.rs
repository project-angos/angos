#[cfg(test)]
mod tests;

use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{
        Arc, Weak,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::registry::metadata_store::{Error, lock::LockBackend};

pub struct MemoryGuard {
    _guards: Vec<OwnedMutexGuard<()>>,
}

#[derive(Debug, Default, Clone)]
pub struct MemoryBackend {
    locks: Arc<Mutex<HashMap<String, Weak<Mutex<()>>>>>,
    counter: Arc<AtomicUsize>,
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self {
            locks: Arc::new(Mutex::new(HashMap::new())),
            counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[cfg(test)]
    pub async fn get_lock_count(&self) -> usize {
        let mut locks = self.locks.lock().await;
        locks.retain(|_, weak| weak.upgrade().is_some());
        locks.len()
    }
}

#[async_trait]
impl LockBackend for MemoryBackend {
    type Guard = Box<dyn Send>;

    async fn acquire(&self, keys: &[String]) -> Result<Self::Guard, Error> {
        let count = self.counter.fetch_add(1, Ordering::Relaxed);

        let mut locks = self.locks.lock().await;
        if count.is_multiple_of(10000) {
            locks.retain(|_, weak| weak.upgrade().is_some());
        }

        let mut mutexes = Vec::with_capacity(keys.len());
        for key in keys {
            let mutex = if let Some(weak) = locks.get(key) {
                if let Some(lock) = weak.upgrade() {
                    lock
                } else {
                    locks.remove(key);
                    let lock = Arc::new(Mutex::new(()));
                    locks.insert(key.clone(), Arc::downgrade(&lock));
                    lock
                }
            } else {
                let lock = Arc::new(Mutex::new(()));
                locks.insert(key.clone(), Arc::downgrade(&lock));
                lock
            };
            mutexes.push(mutex);
        }

        drop(locks);

        let mut guards = Vec::with_capacity(mutexes.len());
        for mutex in mutexes {
            guards.push(mutex.lock_owned().await);
        }

        Ok(Box::new(MemoryGuard { _guards: guards }))
    }
}
