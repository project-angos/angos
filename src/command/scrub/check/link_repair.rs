use std::sync::Arc;

use tracing::debug;

use crate::{
    command::scrub::{action::Action, error::Error, executor::ActionSink},
    oci::{Digest, Namespace},
    registry::metadata_store::{LinkKind, MetadataStore},
};

pub async fn ensure_link(
    metadata_store: &Arc<MetadataStore>,
    namespace: &Namespace,
    link: &LinkKind,
    expected_target: &Digest,
    sink: &mut (dyn ActionSink + Send),
) -> Result<(), Error> {
    match metadata_store.read_link(namespace, link).await {
        Ok(metadata) if &metadata.target == expected_target => {
            debug!("Link {link} -> {expected_target} is valid");
            Ok(())
        }
        _ => {
            debug!("Missing or invalid link: {link} -> {expected_target}");
            sink.apply(Action::RecreateLink {
                namespace: namespace.clone(),
                link: link.clone(),
                target: expected_target.clone(),
            })
            .await
        }
    }
}
