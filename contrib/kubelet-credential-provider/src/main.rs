//! Kubelet credential provider for angos.
//!
//! The kubelet mints a service-account token for the pod being started and
//! hands it to this plugin on stdin; the plugin returns it as the registry
//! password under the `[auth.oidc.<name>]` section name, which is how angos
//! reads a Basic credential as that provider's token.
//!
//! The exchange below mirrors `k8s.io/kubelet/pkg/apis/credentialprovider/v1`,
//! and the `tokenAttributes` that make the kubelet send a token belong to
//! `CredentialProvider` in `k8s.io/kubelet/config/v1`.

use std::collections::HashMap;
use std::io::{Read, Write, stdin, stdout};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use argh::FromArgs;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};

/// Cut off the cached credential before the token it carries expires, so a pull
/// starting on the last cache hit still has a valid token to present.
const EXPIRY_MARGIN_SECS: u64 = 60;

/// Answer one kubelet credential-provider exchange on stdin and stdout.
#[derive(FromArgs)]
struct Arguments {
    /// name of the `[auth.oidc.<name>]` section that validates the token, sent
    /// as the registry username
    #[argh(option)]
    provider: String,

    /// registry host the token may be handed to, refusing any other one a
    /// broader `matchImages` would otherwise route here. Repeat it for every
    /// host angos serves, including one a containerd mirror redirects here
    /// under another name; given none, `matchImages` alone decides
    #[argh(option)]
    registry: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CredentialProviderRequest {
    api_version: String,
    image: String,
    /// Present only when the provider entry sets `tokenAttributes`.
    #[serde(default)]
    service_account_token: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CredentialProviderResponse {
    api_version: String,
    kind: &'static str,
    cache_key_type: &'static str,
    cache_duration: String,
    auth: HashMap<String, AuthConfig>,
}

#[derive(Serialize)]
struct AuthConfig {
    username: String,
    password: String,
}

/// The one claim this plugin reads; the registry checks the rest.
#[derive(Deserialize)]
struct Claims {
    exp: u64,
}

fn main() -> ExitCode {
    let arguments: Arguments = argh::from_env();

    match run(&arguments) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("angos-credential-provider: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(arguments: &Arguments) -> Result<(), String> {
    let mut body = String::new();
    stdin()
        .read_to_string(&mut body)
        .map_err(|e| format!("failed to read the request: {e}"))?;

    let request: CredentialProviderRequest =
        serde_json::from_str(&body).map_err(|e| format!("failed to parse the request: {e}"))?;
    // An unreadable clock reads as `u64::MAX`, which disables caching instead
    // of extending it.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(u64::MAX, |since_epoch| since_epoch.as_secs());
    let response = respond(arguments, &request, now)?;
    let response = serde_json::to_vec(&response)
        .map_err(|e| format!("failed to serialize the response: {e}"))?;

    stdout()
        .write_all(&response)
        .map_err(|e| format!("failed to write the response: {e}"))
}

fn respond(
    arguments: &Arguments,
    request: &CredentialProviderRequest,
    now: u64,
) -> Result<CredentialProviderResponse, String> {
    // The token is a bearer credential the holder can replay against the
    // registry, so a listed host is the one place it may go and a `matchImages`
    // wider than that fails the pull instead. Listing none leaves that call to
    // `matchImages`.
    let registry = registry_of(&request.image);
    if !arguments.registry.is_empty()
        && !arguments
            .registry
            .iter()
            .any(|served| served.eq_ignore_ascii_case(registry))
    {
        return Err(format!(
            "refusing to hand the token to '{registry}': this provider serves {}",
            arguments.registry.join(", ")
        ));
    }

    let Some(token) = &request.service_account_token else {
        return Err(format!(
            "the kubelet sent no service-account token for '{}': set \
             tokenAttributes.serviceAccountTokenAudience on this provider entry",
            request.image
        ));
    };

    let credential = AuthConfig {
        username: arguments.provider.clone(),
        password: token.clone(),
    };

    Ok(CredentialProviderResponse {
        api_version: request.api_version.clone(),
        kind: "CredentialProviderResponse",
        cache_key_type: "Registry",
        cache_duration: cache_duration(token, now),
        auth: HashMap::from([(registry.to_string(), credential)]),
    })
}

/// How long the kubelet may reuse the credential, which is the token's own
/// remaining life less [`EXPIRY_MARGIN_SECS`]: the pass-through credential is
/// worth nothing past its `exp`. A token whose expiry cannot be read caches for
/// no time at all, the zero KEP-4412 defines for plugins returning a token as-is.
fn cache_duration(token: &str, now: u64) -> String {
    let seconds = expiry_of(token)
        .unwrap_or(0)
        .saturating_sub(now.saturating_add(EXPIRY_MARGIN_SECS));
    format!("{seconds}s")
}

/// Read from the token's payload without verifying its signature: the registry
/// is what validates the token, and a forged `exp` only shortens or forfeits
/// this cache entry.
fn expiry_of(token: &str) -> Option<u64> {
    let payload = token.split('.').nth(1)?;
    let payload = URL_SAFE_NO_PAD.decode(payload).ok()?;
    let claims: Claims = serde_json::from_slice(&payload).ok()?;
    Some(claims.exp)
}

/// The registry host the kubelet matched, which keys the returned credential.
fn registry_of(image: &str) -> &str {
    match image.split_once('/') {
        Some((registry, _)) => registry,
        None => image,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_800_000_000;

    fn arguments() -> Arguments {
        Arguments {
            provider: "kube".to_string(),
            registry: vec!["registry.example.com:5000".to_string()],
        }
    }

    fn request(token: Option<&str>) -> CredentialProviderRequest {
        CredentialProviderRequest {
            api_version: "credentialprovider.kubelet.k8s.io/v1".to_string(),
            image: "registry.example.com:5000/team/app:1.0".to_string(),
            service_account_token: token.map(str::to_string),
        }
    }

    fn token_expiring_at(exp: u64) -> String {
        let payload = URL_SAFE_NO_PAD.encode(format!(r#"{{"exp":{exp}}}"#));
        format!("header.{payload}.signature")
    }

    #[test]
    fn the_token_is_returned_under_the_provider_name() {
        let token = token_expiring_at(NOW + 3600);
        let response = respond(&arguments(), &request(Some(&token)), NOW).unwrap();

        assert_eq!(response.api_version, "credentialprovider.kubelet.k8s.io/v1");
        let credential = &response.auth["registry.example.com:5000"];
        assert_eq!(credential.username, "kube");
        assert_eq!(credential.password, token);
    }

    /// The kubelet may reuse the credential for the token's remaining life, so
    /// a pod pulling several images runs the plugin once.
    #[test]
    fn the_credential_is_cached_until_the_token_nears_expiry() {
        let response = respond(
            &arguments(),
            &request(Some(&token_expiring_at(NOW + 3600))),
            NOW,
        )
        .unwrap();

        assert_eq!(response.cache_duration, "3540s");
    }

    /// Anything that leaves the expiry unknown or already reached must not be
    /// held: an expired credential fails the pull it was kept for.
    #[test]
    fn a_token_that_is_spent_or_unreadable_is_not_cached() {
        for token in [
            token_expiring_at(NOW + EXPIRY_MARGIN_SECS),
            token_expiring_at(NOW - 1),
            "not.a.jwt".to_string(),
            "opaque".to_string(),
        ] {
            let response = respond(&arguments(), &request(Some(&token)), NOW).unwrap();

            assert_eq!(response.cache_duration, "0s", "token: {token}");
        }
    }

    /// A clock the plugin cannot read reports `u64::MAX`, which must shorten the
    /// cache rather than overflow into a long one.
    #[test]
    fn an_unreadable_clock_disables_caching() {
        let response = respond(
            &arguments(),
            &request(Some(&token_expiring_at(NOW + 3600))),
            u64::MAX,
        )
        .unwrap();

        assert_eq!(response.cache_duration, "0s");
    }

    #[test]
    fn a_request_without_a_token_names_the_missing_setting() {
        let Err(error) = respond(&arguments(), &request(None), NOW) else {
            panic!("a request carrying no token must not yield a credential");
        };

        assert!(
            error.contains("tokenAttributes.serviceAccountTokenAudience"),
            "got: {error}"
        );
    }

    /// A `matchImages` wider than this provider must cost a failed pull, never
    /// the token: whoever receives it can replay it against the registry.
    #[test]
    fn a_request_for_another_registry_is_refused() {
        let token = token_expiring_at(NOW + 3600);
        let elsewhere = CredentialProviderRequest {
            image: "evil.example.com/team/app:1.0".to_string(),
            ..request(Some(&token))
        };

        let Err(error) = respond(&arguments(), &elsewhere, NOW) else {
            panic!("a registry this provider does not serve must not receive the token");
        };

        assert!(error.contains("evil.example.com"), "got: {error}");
        assert!(
            !error.contains(&token),
            "the error leaked the token: {error}"
        );
    }

    /// A containerd mirror pulls an image whose reference names the mirrored
    /// registry, so every host angos answers for is served.
    #[test]
    fn each_configured_registry_is_served() {
        let arguments = Arguments {
            provider: "kube".to_string(),
            registry: vec![
                "registry.example.com:5000".to_string(),
                "docker.io".to_string(),
            ],
        };
        let token = token_expiring_at(NOW + 3600);
        let mirrored = CredentialProviderRequest {
            image: "docker.io/library/nginx:1.29".to_string(),
            ..request(Some(&token))
        };

        let response = respond(&arguments, &mirrored, NOW).unwrap();

        assert_eq!(response.auth["docker.io"].password, token);
        assert!(
            respond(&arguments, &request(Some(&token)), NOW).is_ok(),
            "the other configured registry must keep working"
        );
    }

    /// Listing no registry defers to `matchImages`, which is what routed the
    /// request here in the first place.
    #[test]
    fn without_a_configured_registry_every_routed_host_is_served() {
        let arguments = Arguments {
            provider: "kube".to_string(),
            registry: Vec::new(),
        };
        let token = token_expiring_at(NOW + 3600);
        let elsewhere = CredentialProviderRequest {
            image: "mirror.example.com/team/app:1.0".to_string(),
            ..request(Some(&token))
        };

        let response = respond(&arguments, &elsewhere, NOW).unwrap();

        assert_eq!(response.auth["mirror.example.com"].password, token);
    }

    /// Hosts are compared as DNS names, so casing in the pod spec is not a
    /// mismatch.
    #[test]
    fn the_registry_host_matches_case_insensitively() {
        let token = token_expiring_at(NOW + 3600);
        let shouted = CredentialProviderRequest {
            image: "Registry.Example.COM:5000/team/app:1.0".to_string(),
            ..request(Some(&token))
        };

        assert!(respond(&arguments(), &shouted, NOW).is_ok());
    }

    #[test]
    fn an_image_without_a_path_is_its_own_registry() {
        assert_eq!(registry_of("registry.example.com"), "registry.example.com");
    }
}
