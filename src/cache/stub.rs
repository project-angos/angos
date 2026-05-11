use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use crate::cache::Error;

/// Test-only cache backend with controllable failure injection and call counts.
///
/// Cloning yields a handle to the same underlying storage, so test code can hold
/// one clone for control/observation while another is wrapped in `Cache::Stub`.
#[derive(Clone, Debug)]
pub struct Backend {
    storage: Arc<Mutex<Storage>>,
    store_calls: Arc<AtomicUsize>,
}

#[derive(Debug, Default)]
struct Storage {
    data: Option<String>,
    retrieve_error: Option<String>,
    store_error: Option<String>,
}

impl Backend {
    pub fn new() -> Self {
        Self {
            storage: Arc::new(Mutex::new(Storage::default())),
            store_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn set_data(&self, data: Option<String>) {
        self.storage.lock().unwrap().data = data;
    }

    pub fn set_retrieve_error(&self, error: Option<String>) {
        self.storage.lock().unwrap().retrieve_error = error;
    }

    pub fn set_store_error(&self, error: Option<String>) {
        self.storage.lock().unwrap().store_error = error;
    }

    pub fn store_calls(&self) -> usize {
        self.store_calls.load(Ordering::Relaxed)
    }

    pub fn store_value(&self, _key: &str, value: &str, _ttl: u64) -> Result<(), Error> {
        self.store_calls.fetch_add(1, Ordering::Relaxed);
        let mut storage = self.storage.lock().unwrap();
        if let Some(error) = &storage.store_error {
            return Err(Error::Execution(error.clone()));
        }
        storage.data = Some(value.to_string());
        Ok(())
    }

    pub fn retrieve_value(&self, _key: &str) -> Result<Option<String>, Error> {
        let storage = self.storage.lock().unwrap();
        if let Some(error) = &storage.retrieve_error {
            return Err(Error::Execution(error.clone()));
        }
        Ok(storage.data.clone())
    }

    pub fn delete_value(&self, _key: &str) {
        self.storage.lock().unwrap().data = None;
    }
}
