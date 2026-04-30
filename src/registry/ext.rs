use std::collections::HashMap;

use chrono::{DateTime, Utc};
use hyper::header::CONTENT_TYPE;
use serde::Serialize;
use tracing::instrument;

use crate::{
    configuration::RegexPattern,
    oci::{Descriptor, Digest, Manifest, Namespace, Platform as OciPlatform, namespace_belongs_to},
    registry::{
        Error, JsonResponse, Registry, metadata_store::link_kind::LinkKind,
        pagination::collect_all_pages,
    },
};

const APPLICATION_JSON: &str = "application/json";

fn json_headers() -> HashMap<&'static str, String> {
    HashMap::from([(CONTENT_TYPE.as_str(), APPLICATION_JSON.to_string())])
}

#[derive(Serialize, Debug)]
struct RepositoryInfo {
    name: String,
    namespace_count: usize,
    pull_through_cache: bool,
    immutable_tags: bool,
}

#[derive(Serialize, Debug)]
struct RepositoriesBody {
    repositories: Vec<RepositoryInfo>,
}

#[derive(Serialize, Debug)]
struct NamespaceInfo {
    name: String,
    manifest_count: usize,
    upload_count: usize,
}

#[derive(Serialize, Debug)]
struct NamespacesBody {
    repository: String,
    namespaces: Vec<NamespaceInfo>,
    pull_through_cache: bool,
    upstream_urls: Vec<String>,
    immutable_tags: bool,
    immutable_tags_exclusions: Vec<RegexPattern>,
}

#[derive(Serialize, Debug, Clone)]
struct Platform {
    os: String,
    architecture: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    variant: Option<String>,
}

impl From<OciPlatform> for Platform {
    fn from(p: OciPlatform) -> Self {
        Platform {
            os: p.os,
            architecture: p.architecture,
            variant: p.variant,
        }
    }
}

#[derive(Serialize, Debug, Clone)]
struct ParentRef {
    digest: String,
    tags: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    platform: Option<Platform>,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct ReferrerInfo {
    digest: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    artifact_type: Option<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    annotations: HashMap<String, String>,
}

impl From<Descriptor> for ReferrerInfo {
    fn from(descriptor: Descriptor) -> Self {
        Self {
            digest: descriptor.digest.to_string(),
            artifact_type: descriptor.artifact_type,
            annotations: descriptor.annotations,
        }
    }
}

#[derive(Serialize, Debug)]
struct ManifestEntry {
    digest: String,
    tags: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    parents: Vec<ParentRef>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    referrers: Vec<ReferrerInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pushed_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_pulled_at: Option<DateTime<Utc>>,
}

#[derive(Serialize, Debug)]
struct RevisionsBody {
    name: String,
    manifests: Vec<ManifestEntry>,
}

#[derive(Serialize, Debug)]
struct UploadEntry {
    uuid: String,
    size: u64,
    started_at: DateTime<Utc>,
}

#[derive(Serialize, Debug)]
struct UploadsBody {
    name: String,
    uploads: Vec<UploadEntry>,
}

struct RepositoryConfig {
    pull_through_cache: bool,
    upstream_urls: Vec<String>,
    immutable_tags: bool,
    immutable_tags_exclusions: Vec<RegexPattern>,
}

fn apply_predicate_type(child_manifest: &Manifest, annotations: &mut HashMap<String, String>) {
    const ANNOTATION_PREDICATE_TYPE: &str = "in-toto.io/predicate-type";

    if let Some(predicate_type) = child_manifest
        .layers
        .iter()
        .find_map(|layer| layer.annotations.get(ANNOTATION_PREDICATE_TYPE).cloned())
    {
        annotations.insert(ANNOTATION_PREDICATE_TYPE.to_string(), predicate_type);
    }
}

/// If the child descriptor has a parseable Docker referrer subject, returns it;
/// otherwise treats the descriptor as a regular index child and inserts a parent
/// entry. A descriptor whose annotation is present but unparseable is silently
/// skipped (no parent, no referrer).
fn resolve_referrer_subject(
    parent_digest: &Digest,
    child_descriptor: &Descriptor,
    child_to_parents: &mut HashMap<Digest, Vec<(Digest, Option<Platform>)>>,
) -> Option<Digest> {
    const ANNOTATION_DOCKER_REFERENCE_DIGEST: &str = "vnd.docker.reference.digest";

    let Some(subject_str) = child_descriptor
        .annotations
        .get(ANNOTATION_DOCKER_REFERENCE_DIGEST)
    else {
        let platform = child_descriptor.platform.clone().map(Platform::from);
        child_to_parents
            .entry(child_descriptor.digest.clone())
            .or_default()
            .push((parent_digest.clone(), platform));
        return None;
    };

    subject_str.parse::<Digest>().ok()
}

impl Registry {
    #[instrument(skip(self))]
    pub async fn get_repositories_info(&self) -> Result<JsonResponse, Error> {
        let mut repositories = Vec::with_capacity(self.repositories.len());

        for name in self.repositories.keys() {
            let namespaces = self.list_repository_namespaces(name).await?;
            let config = self.get_repository_config(name);
            repositories.push(RepositoryInfo {
                name: name.clone(),
                namespace_count: namespaces.len(),
                pull_through_cache: config.pull_through_cache,
                immutable_tags: config.immutable_tags,
            });
        }

        repositories.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(JsonResponse {
            headers: json_headers(),
            body: serde_json::to_vec(&RepositoriesBody { repositories })?,
        })
    }

    #[instrument(skip(self))]
    pub async fn get_namespaces_info(&self, repository: &str) -> Result<JsonResponse, Error> {
        let namespace_names = self.list_repository_namespaces(repository).await?;
        let mut namespaces = Vec::with_capacity(namespace_names.len());

        for name_str in namespace_names {
            let name = Namespace::new(&name_str).map_err(|_| Error::NameInvalid)?;
            let manifest_count = self.metadata_store.count_manifests(&name).await?;
            let upload_count = self.count_uploads(&name).await?;
            namespaces.push(NamespaceInfo {
                name: name_str,
                manifest_count,
                upload_count,
            });
        }

        let config = self.get_repository_config(repository);

        Ok(JsonResponse {
            headers: json_headers(),
            body: serde_json::to_vec(&NamespacesBody {
                repository: repository.to_string(),
                namespaces,
                pull_through_cache: config.pull_through_cache,
                upstream_urls: config.upstream_urls,
                immutable_tags: config.immutable_tags,
                immutable_tags_exclusions: config.immutable_tags_exclusions,
            })?,
        })
    }

    #[instrument(skip(self))]
    pub async fn get_revisions_info(&self, namespace: &Namespace) -> Result<JsonResponse, Error> {
        let all_revisions = collect_all_pages(|token| async move {
            self.metadata_store
                .list_revisions(namespace, 1000, token)
                .await
                .map_err(Error::from)
        })
        .await?;
        let digest_to_tags = self.build_digest_to_tags_map(namespace).await?;
        let (child_to_parents, docker_referrers) =
            self.build_parent_and_referrer_maps(&all_revisions).await;
        let manifests = self
            .build_manifest_entries(
                namespace,
                all_revisions,
                &digest_to_tags,
                child_to_parents,
                docker_referrers,
            )
            .await;

        Ok(JsonResponse {
            headers: json_headers(),
            body: serde_json::to_vec(&RevisionsBody {
                name: namespace.to_string(),
                manifests,
            })?,
        })
    }

    #[instrument(skip(self))]
    pub async fn get_uploads_info(&self, namespace: &Namespace) -> Result<JsonResponse, Error> {
        let uuids = collect_all_pages(|token| async move {
            self.upload_store
                .list(namespace, 1000, token)
                .await
                .map_err(Error::from)
        })
        .await?;

        let mut all_uploads = Vec::new();
        for uuid in uuids {
            if let Ok(summary) = self.upload_store.summary(namespace, &uuid).await {
                all_uploads.push(UploadEntry {
                    uuid,
                    size: summary.size,
                    started_at: summary.started_at,
                });
            }
        }

        Ok(JsonResponse {
            headers: json_headers(),
            body: serde_json::to_vec(&UploadsBody {
                name: namespace.to_string(),
                uploads: all_uploads,
            })?,
        })
    }

    fn get_repository_config(&self, name: &str) -> RepositoryConfig {
        let global_exclusions = || self.global_immutable_tags_exclusions.clone();

        let Some(repo) = self.repositories.get(name) else {
            return RepositoryConfig {
                pull_through_cache: false,
                upstream_urls: Vec::new(),
                immutable_tags: self.global_immutable_tags,
                immutable_tags_exclusions: global_exclusions(),
            };
        };

        let upstream_urls: Vec<String> = repo.upstreams.iter().map(|u| u.url.clone()).collect();
        let immutable_tags_exclusions = if repo.immutable_tags_exclusions.is_empty() {
            global_exclusions()
        } else {
            repo.immutable_tags_exclusions.clone()
        };
        RepositoryConfig {
            pull_through_cache: !upstream_urls.is_empty(),
            upstream_urls,
            immutable_tags: repo.immutable_tags || self.global_immutable_tags,
            immutable_tags_exclusions,
        }
    }

    async fn build_parent_and_referrer_maps(
        &self,
        all_revisions: &[Digest],
    ) -> (
        HashMap<Digest, Vec<(Digest, Option<Platform>)>>,
        HashMap<Digest, Vec<ReferrerInfo>>,
    ) {
        let mut child_to_parents: HashMap<Digest, Vec<(Digest, Option<Platform>)>> = HashMap::new();
        let mut docker_referrers: HashMap<Digest, Vec<ReferrerInfo>> = HashMap::new();

        for digest in all_revisions {
            let Some(manifest) = self.read_manifest(digest).await else {
                continue;
            };

            for child_descriptor in &manifest.manifests {
                let Some(subject) =
                    resolve_referrer_subject(digest, child_descriptor, &mut child_to_parents)
                else {
                    continue;
                };

                let mut annotations = child_descriptor.annotations.clone();
                if let Some(child_manifest) = self.read_manifest(&child_descriptor.digest).await {
                    apply_predicate_type(&child_manifest, &mut annotations);
                }
                docker_referrers
                    .entry(subject)
                    .or_default()
                    .push(ReferrerInfo {
                        digest: child_descriptor.digest.to_string(),
                        artifact_type: child_descriptor.artifact_type.clone(),
                        annotations,
                    });
            }
        }

        (child_to_parents, docker_referrers)
    }

    async fn build_manifest_entries(
        &self,
        namespace: &Namespace,
        all_revisions: Vec<Digest>,
        digest_to_tags: &HashMap<Digest, Vec<String>>,
        child_to_parents: HashMap<Digest, Vec<(Digest, Option<Platform>)>>,
        mut docker_referrers: HashMap<Digest, Vec<ReferrerInfo>>,
    ) -> Vec<ManifestEntry> {
        let mut manifests: Vec<ManifestEntry> = Vec::with_capacity(all_revisions.len());

        for digest in all_revisions {
            let tags = digest_to_tags.get(&digest).cloned().unwrap_or_default();
            let parents = child_to_parents
                .get(&digest)
                .map(|parents| {
                    parents
                        .iter()
                        .map(|(parent_digest, platform)| ParentRef {
                            digest: parent_digest.to_string(),
                            tags: digest_to_tags
                                .get(parent_digest)
                                .cloned()
                                .unwrap_or_default(),
                            platform: platform.clone(),
                        })
                        .collect()
                })
                .unwrap_or_default();

            let mut referrers: Vec<ReferrerInfo> =
                docker_referrers.remove(&digest).unwrap_or_default();

            if let Ok(oci_referrers) = self.list_referrers(namespace, &digest, None).await {
                referrers.extend(oci_referrers.into_iter().map(ReferrerInfo::from));
            }

            let (pushed_at, last_pulled_at) = self
                .metadata_store
                .read_link(namespace, &LinkKind::Digest(digest.clone()), false)
                .await
                .map_or((None, None), |m| (m.created_at, m.accessed_at));

            manifests.push(ManifestEntry {
                digest: digest.to_string(),
                tags,
                parents,
                referrers,
                pushed_at,
                last_pulled_at,
            });
        }

        manifests
    }

    async fn count_uploads(&self, namespace: &Namespace) -> Result<usize, Error> {
        let uploads = collect_all_pages(|token| async move {
            self.upload_store
                .list(namespace, 1000, token)
                .await
                .map_err(Error::from)
        })
        .await?;
        Ok(uploads.len())
    }

    async fn build_digest_to_tags_map(
        &self,
        namespace: &Namespace,
    ) -> Result<HashMap<Digest, Vec<String>>, Error> {
        let all_tags = collect_all_pages(|last| async move {
            self.metadata_store
                .list_tags(namespace, 1000, last)
                .await
                .map_err(Error::from)
        })
        .await?;

        let mut digest_to_tags: HashMap<Digest, Vec<String>> = HashMap::new();

        for tag in all_tags {
            let link = LinkKind::Tag(tag.clone());
            if let Ok(link_metadata) = self.metadata_store.read_link(namespace, &link, false).await
            {
                digest_to_tags
                    .entry(link_metadata.target)
                    .or_default()
                    .push(tag);
            }
        }

        Ok(digest_to_tags)
    }

    async fn list_repository_namespaces(&self, repository: &str) -> Result<Vec<String>, Error> {
        if !self.repositories.contains_key(repository) {
            return Err(Error::NameUnknown);
        }

        let all_namespaces = collect_all_pages(|token| async move {
            self.metadata_store
                .list_namespaces(1000, token)
                .await
                .map_err(Error::from)
        })
        .await?;

        let mut matching_namespaces: Vec<String> = all_namespaces
            .into_iter()
            .filter(|ns| namespace_belongs_to(ns, repository))
            .collect();

        matching_namespaces.sort_unstable();
        Ok(matching_namespaces)
    }

    async fn read_manifest(&self, digest: &Digest) -> Option<Manifest> {
        let blob = self.blob_store.read(digest).await.ok()?;
        Manifest::from_slice(&blob).ok()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::apply_predicate_type;
    use crate::oci::{Descriptor, Digest, Manifest};

    const ANNOTATION_PREDICATE_TYPE: &str = "in-toto.io/predicate-type";

    fn test_digest() -> Digest {
        "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            .parse()
            .unwrap()
    }

    fn manifest_with_layers(layer_annotations: Vec<HashMap<String, String>>) -> Manifest {
        let layers: Vec<Descriptor> = layer_annotations
            .into_iter()
            .map(|ann| Descriptor {
                media_type: "application/vnd.oci.image.layer.v1.tar+gzip".to_string(),
                digest: test_digest(),
                size: 0,
                annotations: ann,
                artifact_type: None,
                platform: None,
            })
            .collect();
        Manifest {
            layers,
            ..Manifest::default()
        }
    }

    #[test]
    fn apply_predicate_type_noop_for_zero_layers() {
        let manifest = manifest_with_layers(vec![]);
        let mut annotations = HashMap::new();
        apply_predicate_type(&manifest, &mut annotations);
        assert!(annotations.is_empty());
    }

    #[test]
    fn apply_predicate_type_noop_when_annotation_absent() {
        let manifest = manifest_with_layers(vec![HashMap::from([(
            "some.other.key".to_string(),
            "value".to_string(),
        )])]);
        let mut annotations = HashMap::new();
        apply_predicate_type(&manifest, &mut annotations);
        assert!(annotations.is_empty());
    }

    #[test]
    fn apply_predicate_type_inserts_first_when_multiple_layers_have_it() {
        let manifest = manifest_with_layers(vec![
            HashMap::from([(
                ANNOTATION_PREDICATE_TYPE.to_string(),
                "https://slsa.dev/provenance/v0.2".to_string(),
            )]),
            HashMap::from([(
                ANNOTATION_PREDICATE_TYPE.to_string(),
                "https://slsa.dev/provenance/v1".to_string(),
            )]),
        ]);
        let mut annotations = HashMap::new();
        apply_predicate_type(&manifest, &mut annotations);
        assert_eq!(
            annotations.get(ANNOTATION_PREDICATE_TYPE),
            Some(&"https://slsa.dev/provenance/v0.2".to_string())
        );
    }

    #[test]
    fn apply_predicate_type_inserts_when_single_layer_has_it() {
        let manifest = manifest_with_layers(vec![HashMap::from([(
            ANNOTATION_PREDICATE_TYPE.to_string(),
            "https://slsa.dev/provenance/v0.2".to_string(),
        )])]);
        let mut annotations = HashMap::new();
        apply_predicate_type(&manifest, &mut annotations);
        assert_eq!(
            annotations.get(ANNOTATION_PREDICATE_TYPE),
            Some(&"https://slsa.dev/provenance/v0.2".to_string())
        );
    }
}
