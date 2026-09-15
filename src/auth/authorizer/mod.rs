use std::{collections::HashMap, path::PathBuf, sync::Arc};

use http::request::Parts;
use reqwest::{Client, redirect::Policy};
use tracing::{debug, info, instrument, warn};

use angos_oci::Namespace;
use angos_oci::request::BlobMount;

use crate::{
    auth::Error,
    auth::webhook::{self, WebhookAuthorizer},
    configuration::Configuration,
    identity::{Action, ClientIdentity},
    policy::{AccessPolicy, PolicyDecision},
    registry::{Registry, Repository},
};
use angos_cache::Cache;
use angos_mtls_client::MtlsClientBuilder;

const ACCESS_DENIED: &str = "Access denied";

/// Centralized authorization component that handles all access control decisions
pub struct Authorizer {
    global_access_policy: AccessPolicy,
    global_authorization_webhook: Option<Arc<WebhookAuthorizer>>,
    repositories: HashMap<String, AuthorizerRepository>,
}

/// Repository-specific authorization configuration
struct AuthorizerRepository {
    access_policy: Option<AccessPolicy>,
    authorization_webhook: Option<Arc<WebhookAuthorizer>>,
}

/// The TLS files a webhook client is built from; webhooks sharing them share
/// the client.
type WebhookClientFiles = (Option<PathBuf>, Option<PathBuf>, Option<PathBuf>);

impl Authorizer {
    pub fn new(config: &Configuration, cache: &Arc<Cache>) -> Result<Self, Error> {
        let global_access_policy = AccessPolicy::new(config.global.access_policy.clone());

        let webhook_authorizers = build_webhooks(config, cache)?;

        let global_authorization_webhook = config
            .global
            .authorization_webhook
            .as_ref()
            .map(|name| lookup_webhook(&webhook_authorizers, name))
            .transpose()?;

        let repositories = build_repositories(config, &webhook_authorizers)?;

        Ok(Self {
            global_access_policy,
            global_authorization_webhook,
            repositories,
        })
    }

    #[instrument(skip(self, request, registry, identity))]
    pub async fn authorize_request(
        &self,
        action: &Action,
        identity: &ClientIdentity,
        request: &Parts,
        registry: &Registry,
    ) -> Result<(), Error> {
        let namespace = action.get_namespace();
        let repository =
            namespace.and_then(|namespace| registry.get_repository_for_namespace(namespace).ok());

        debug!("Evaluating global access policy");
        enforce_policy(
            self.global_access_policy.evaluate(
                action,
                identity,
                self.has_repository_policy(repository),
            ),
            "global",
            identity,
        )?;

        // A namespace no `[repository]` declares has no repository policy and no
        // repository webhook, so the global webhook gates it as it does a request
        // carrying no namespace at all.
        if let (Some(repository), Some(namespace)) = (repository, namespace) {
            self.authorize_namespace_request(repository, namespace, action, identity, request)
                .await?;
        } else if let Some(webhook) = &self.global_authorization_webhook {
            enforce_webhook(webhook, "global webhook", action, identity, request).await?;
        }

        Ok(())
    }

    /// Resolves a source namespace from which `identity` may already read
    /// `mount.digest`: the precondition for granting a cross-repo blob mount.
    /// Without it a caller could mount, then read, any blob held by any other
    /// namespace, bypassing the source's read policy; `None` means degrade to a
    /// normal upload session.
    #[instrument(skip(self, request, registry, identity))]
    pub async fn authorize_mount_source(
        &self,
        mount: &BlobMount,
        identity: &ClientIdentity,
        request: &Parts,
        registry: &Registry,
    ) -> Result<Option<Namespace>, Error> {
        for source in registry.mount_source_candidates(mount).await? {
            let action = Action::GetBlob {
                namespace: source.clone(),
                digest: mount.digest.clone(),
            };
            match self
                .authorize_request(&action, identity, request, registry)
                .await
            {
                Ok(()) => return Ok(Some(source)),
                Err(Error::Unauthorized(_)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(None)
    }

    async fn authorize_namespace_request(
        &self,
        repository: &Repository,
        namespace: &Namespace,
        action: &Action,
        identity: &ClientIdentity,
        request: &Parts,
    ) -> Result<(), Error> {
        debug!(
            "Evaluating repository access policy for namespace: {namespace} ({})",
            repository.name
        );

        let auth_repo = self
            .repositories
            .get(repository.name.as_ref())
            .ok_or_else(|| {
                Error::Execution(format!(
                    "Repository '{}' not found in authorizer",
                    repository.name
                ))
            })?;

        if let Some(access_policy) = &auth_repo.access_policy {
            enforce_policy(
                access_policy.evaluate(action, identity, true),
                &format!("repository '{}'", repository.name),
                identity,
            )?;
        }

        // Reject pull-through pushes before the paid webhook round-trip: a
        // pull-through cache never accepts writes whatever a webhook would say.
        if repository.is_pull_through() && action.is_push() {
            return Err(Error::Unauthorized(
                "Push operations are not supported on pull-through cache repositories".to_string(),
            ));
        }

        let webhook = auth_repo
            .authorization_webhook
            .as_ref()
            .or(self.global_authorization_webhook.as_ref());
        if let Some(webhook) = webhook {
            enforce_webhook(webhook, "webhook", action, identity, request).await?;
        }

        Ok(())
    }

    /// Whether `identity` may see `namespace` in the catalog listing under the
    /// access policies: the global policy, then the covering repository's when
    /// it declares one. The webhook is deliberately not consulted, since one
    /// call per entry per page would be prohibitive, and hiding a repository is
    /// a policy-visibility concern rather than a per-request authorization.
    #[must_use]
    pub fn allows_catalog_entry(
        &self,
        namespace: &Namespace,
        identity: &ClientIdentity,
        registry: &Registry,
    ) -> bool {
        let repository = registry.get_repository_for_namespace(namespace).ok();
        // A repository is listable when the client could list its tags: the
        // catalog is a listing, so it gates on the same read.
        let action = Action::ListTags {
            namespace: namespace.clone(),
            n: None,
            last: None,
        };
        let global = self.global_access_policy.evaluate(
            &action,
            identity,
            self.has_repository_policy(repository),
        );
        if !matches!(global, PolicyDecision::Allow) {
            return false;
        }
        let Some(repository) = repository else {
            return true;
        };
        match self.repositories.get(repository.name.as_ref()) {
            Some(auth_repo) => auth_repo.access_policy.as_ref().is_none_or(|policy| {
                matches!(
                    policy.evaluate(&action, identity, true),
                    PolicyDecision::Allow
                )
            }),
            None => true,
        }
    }

    /// Whether the `[repository]` covering the request carries its own access
    /// policy, so that policy decides it after the global one. Global rules read
    /// it as `has_repository_policy()` to hand the decision over instead of
    /// restating every repository rule globally.
    fn has_repository_policy(&self, repository: Option<&Repository>) -> bool {
        repository
            .and_then(|repository| self.repositories.get(repository.name.as_ref()))
            .is_some_and(|auth_repo| auth_repo.access_policy.is_some())
    }
}

fn build_webhooks(
    config: &Configuration,
    cache: &Arc<Cache>,
) -> Result<HashMap<String, Arc<WebhookAuthorizer>>, Error> {
    let mut webhooks = HashMap::with_capacity(config.auth.webhook.len());
    let mut clients: HashMap<WebhookClientFiles, Client> = HashMap::new();
    for (name, webhook_config) in &config.auth.webhook {
        let client_config = (
            webhook_config.server_ca_bundle.clone(),
            webhook_config.client_certificate_bundle.clone(),
            webhook_config.client_private_key.clone(),
        );
        let client = if let Some(client) = clients.get(&client_config) {
            client.clone()
        } else {
            let client = build_webhook_client(webhook_config).map_err(|e| {
                Error::Initialization(format!("Failed to create webhook '{name}': {e}"))
            })?;
            clients.insert(client_config, client.clone());
            client
        };
        let authorizer =
            WebhookAuthorizer::new(name.clone(), webhook_config.clone(), client, cache.clone())
                .map_err(|e| {
                    Error::Initialization(format!("Failed to create webhook '{name}': {e}"))
                })?;
        webhooks.insert(name.clone(), Arc::new(authorizer));
    }
    Ok(webhooks)
}

fn build_webhook_client(config: &webhook::Config) -> Result<Client, String> {
    MtlsClientBuilder::new()
        .with_redirect_policy(Policy::none())
        .with_server_ca_bundle(config.server_ca_bundle.as_deref())
        .with_client_certificate((
            config.client_certificate_bundle.as_deref(),
            config.client_private_key.as_deref(),
        ))
        .build()
}

fn build_repositories(
    config: &Configuration,
    webhook_authorizers: &HashMap<String, Arc<WebhookAuthorizer>>,
) -> Result<HashMap<String, AuthorizerRepository>, Error> {
    let mut repositories = HashMap::with_capacity(config.repository.len());
    for (name, repo_config) in &config.repository {
        let access_policy = repo_config.access_policy.clone().map(AccessPolicy::new);

        let authorization_webhook = repo_config
            .authorization_webhook_ref()
            .map(|name| lookup_webhook(webhook_authorizers, name))
            .transpose()?;

        repositories.insert(
            name.clone(),
            AuthorizerRepository {
                access_policy,
                authorization_webhook,
            },
        );
    }
    Ok(repositories)
}

/// Map a policy `decision` for `scope` (e.g. `"global"`, `"repository 'foo'"`)
/// to an authorization result, logging and denying on `Deny` or
/// `Indeterminate`.
fn enforce_policy(
    decision: PolicyDecision,
    scope: &str,
    identity: &ClientIdentity,
) -> Result<(), Error> {
    match decision {
        PolicyDecision::Allow => Ok(()),
        PolicyDecision::Deny => {
            log_denial(&format!("{scope} policy"), identity);
            Err(Error::Unauthorized(ACCESS_DENIED.to_string()))
        }
        PolicyDecision::Indeterminate(err) => {
            warn!("{scope} access policy indeterminate, denying: {err}");
            log_denial(&format!("{scope} policy (indeterminate)"), identity);
            Err(Error::Unauthorized(ACCESS_DENIED.to_string()))
        }
    }
}

/// Evaluate `webhook` for the request, denying when it does not grant access.
/// `label` names the scope in the logs (e.g. `"webhook"`, `"global webhook"`).
async fn enforce_webhook(
    webhook: &WebhookAuthorizer,
    label: &str,
    action: &Action,
    identity: &ClientIdentity,
    request: &Parts,
) -> Result<(), Error> {
    debug!("Evaluating {label} authorization: {}", webhook.name());
    if webhook.authorize(action, identity, request).await? {
        return Ok(());
    }
    log_denial(&format!("{label} '{}'", webhook.name()), identity);
    Err(Error::Unauthorized(ACCESS_DENIED.to_string()))
}

/// The audit line names the authenticator's one classification, so it and the
/// request span never disagree.
fn log_denial(reason: &str, identity: &ClientIdentity) {
    info!(
        "Access denied: {reason} | Identity: auth_type={} id={:?} username={:?} client_ip={:?} \
         certificate_organizations={:?} certificate_common_names={:?} oidc_provider_name={:?}",
        identity.auth_method.as_str(),
        identity.id,
        identity.username,
        identity.client_ip,
        identity.certificate.organizations,
        identity.certificate.common_names,
        identity.oidc.as_ref().map(|oidc| &oidc.provider_name),
    );
}

fn lookup_webhook(
    authorizers: &HashMap<String, Arc<WebhookAuthorizer>>,
    name: &str,
) -> Result<Arc<WebhookAuthorizer>, Error> {
    authorizers.get(name).cloned().ok_or_else(|| {
        Error::Initialization(format!(
            "Internal: webhook '{name}' missing from authorizer map",
        ))
    })
}

#[cfg(test)]
mod tests;
