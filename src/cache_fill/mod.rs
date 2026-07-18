//! Pull-through cache-fill: durable jobs that fetch upstream blobs into the
//! local blob store and grant the pulling namespace a reference. Distinct from
//! [`crate::cache`], the TTL key-value cache backend.

mod handler;

#[cfg(test)]
pub use crate::cache_fill::handler::CACHE_FETCH_BLOB_KIND;
pub use crate::cache_fill::handler::{
    CACHE_ACTOR, CacheFetchBlobPayload, CacheFillJobHandler, build_envelope,
};
