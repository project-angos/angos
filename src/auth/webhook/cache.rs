use std::sync::Arc;

use crate::{
    cache::{Cache, CacheExt},
    command::server::Error,
};

pub async fn cache_retrieve(
    cache: &Arc<dyn Cache>,
    name: &str,
    cache_key: &str,
) -> Result<Option<bool>, Error> {
    let Ok(Some(cached)) = cache.retrieve::<bool>(cache_key).await else {
        return Ok(None);
    };

    let label = if cached {
        "cached_allow"
    } else {
        "cached_deny"
    };

    super::authorizer::WEBHOOK_REQUESTS
        .with_label_values(&[name, label])
        .inc();

    Ok(Some(cached))
}
