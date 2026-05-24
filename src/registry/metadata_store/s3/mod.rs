use std::sync::Arc;

use tracing::{info, warn};

use crate::{
    cache::Cache,
    registry::metadata_store::{
        Backend, ConditionalCapabilities, Error, LockStrategy,
        backend::Coordinator,
        lock::{self, MemoryBackend, S3LockBackend},
    },
};
use angos_s3_client as s3_client;
use angos_storage::s3::Backend as StorageS3Backend;

pub mod config;
mod probe;

pub use config::BackendConfig;
pub use probe::probe_conditional_capabilities;

impl BackendConfig {
    /// Build the unified metadata-store backend for an S3 deployment.
    ///
    /// `conditional` carries the detected (or explicitly declared) S3
    /// conditional-write capabilities. When `None` the capabilities are
    /// derived from the configured `lock_strategy`.
    pub fn to_backend(
        &self,
        conditional: Option<ConditionalCapabilities>,
        cache: Option<Arc<Cache>>,
    ) -> Result<Backend, Error> {
        info!("Using S3 metadata-store backend");

        let caps = conditional.unwrap_or_else(|| {
            if matches!(self.lock_strategy, LockStrategy::S3(_)) {
                ConditionalCapabilities {
                    put_if_none_match: true,
                    put_if_match: true,
                    delete_if_match: true,
                }
            } else {
                ConditionalCapabilities::default()
            }
        });

        if self.access_time_debounce_secs == 0 && matches!(self.lock_strategy, LockStrategy::S3(_))
        {
            warn!(
                "access_time_debounce_secs is 0 with S3 lock strategy; \
                 every manifest pull will trigger a synchronous access time update \
                 (CAS loop, with lock fallback), adding S3 API latency. \
                 Consider setting access_time_debounce_secs to 60 or higher."
            );
        }

        let http = s3_client::Backend::new(&self.to_data_store_config())?;
        let storage = Arc::new(StorageS3Backend::builder().client(Arc::new(http)).build()?);

        let coordinator = match &self.lock_strategy {
            LockStrategy::S3(s3_lock_config) => {
                if !caps.supports_cas() {
                    return Err(Error::Lock(format!(
                        "S3 lock strategy requires If-None-Match and If-Match support, \
                         but provider has put_if_none_match={}, put_if_match={}. \
                         Use lock_strategy = redis or lock_strategy = memory instead.",
                        caps.put_if_none_match, caps.put_if_match
                    )));
                }
                info!("Using CAS coordinator with S3 lock for S3 metadata-store");
                let lock_http = s3_client::Backend::new(&self.to_lock_store_config(s3_lock_config))
                    .map_err(|e| Error::Lock(format!("Failed to initialize S3 lock store: {e}")))?;
                let lock =
                    Arc::new(
                        S3LockBackend::new(
                            Arc::new(lock_http),
                            s3_lock_config,
                            caps.delete_if_match,
                        )
                        .map_err(|e| {
                            Error::Lock(format!("Failed to initialize S3 lock store: {e}"))
                        })?,
                    );
                Arc::new(Coordinator::Cas {
                    store: storage,
                    lock,
                })
            }
            LockStrategy::Redis(redis_config) => {
                info!("Using Redis lock store for S3 metadata-store");
                let lock = lock::RedisBackend::new(redis_config).map_err(|e| {
                    Error::Lock(format!("Failed to initialize Redis lock store: {e}"))
                })?;
                Arc::new(Coordinator::Locked {
                    store: storage,
                    lock: Arc::new(lock),
                })
            }
            LockStrategy::Memory => {
                info!("Using in-memory lock store for S3 metadata-store");
                Arc::new(Coordinator::Locked {
                    store: storage,
                    lock: Arc::new(MemoryBackend::new()),
                })
            }
        };

        let mut builder = Backend::builder()
            .coordinator(coordinator)
            .link_cache_ttl(self.link_cache_ttl)
            .access_time_debounce_secs(self.access_time_debounce_secs);

        if let Some(c) = cache {
            builder = builder.cache(c);
        }

        builder.build()
    }
}
