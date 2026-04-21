use std::collections::HashMap;

use chrono::{DateTime, Utc};
use hyper::header::CONTENT_TYPE;
use serde::Serialize;
use tracing::instrument;

use crate::{
    oci::{Digest, Manifest, Namespace, Platform as OciPlatform},
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
    immutable_tags_exclusions: Vec<String>,
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
    immutable_tags_exclusions: Vec<String>,
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
            self.blob_store
                .list_uploads(namespace, 1000, token)
                .await
                .map_err(Error::from)
        })
        .await?;

        let mut all_uploads = Vec::new();
        for uuid in uuids {
            if let Ok((_, size, started_at)) =
                self.blob_store.read_upload_summary(namespace, &uuid).await
            {
                all_uploads.push(UploadEntry {
                    uuid,
                    size,
                    started_at,
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
        if let Some(repo) = self.repositories.get(name) {
            let upstream_urls: Vec<String> = repo.upstreams.iter().map(|u| u.url.clone()).collect();
            let pull_through_cache = !upstream_urls.is_empty();
            let immutable_tags = repo.immutable_tags || self.global_immutable_tags;
            let immutable_tags_exclusions = if repo.immutable_tags_exclusions.is_empty() {
                self.global_immutable_tags_exclusions.clone()
            } else {
                repo.immutable_tags_exclusions.clone()
            };
            RepositoryConfig {
                pull_through_cache,
                upstream_urls,
                immutable_tags,
                immutable_tags_exclusions,
            }
        } else {
            RepositoryConfig {
                pull_through_cache: false,
                upstream_urls: Vec::new(),
                immutable_tags: self.global_immutable_tags,
                immutable_tags_exclusions: self.global_immutable_tags_exclusions.clone(),
            }
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
            let Ok(blob_data) = self.blob_store.read_blob(digest).await else {
                continue;
            };
            let Ok(manifest) = Manifest::from_slice(&blob_data) else {
                continue;
            };

            for child_descriptor in &manifest.manifests {
                if let Some(subject_digest_str) = child_descriptor
                    .annotations
                    .get("vnd.docker.reference.digest")
                {
                    if let Ok(subject_digest) = subject_digest_str.parse::<Digest>() {
                        let mut annotations = child_descriptor.annotations.clone();
                        if let Ok(child_blob) =
                            self.blob_store.read_blob(&child_descriptor.digest).await
                            && let Ok(child_manifest) = Manifest::from_slice(&child_blob)
                        {
                            for layer in &child_manifest.layers {
                                if let Some(predicate_type) =
                                    layer.annotations.get("in-toto.io/predicate-type")
                                {
                                    annotations.insert(
                                        "in-toto.io/predicate-type".to_string(),
                                        predicate_type.clone(),
                                    );
                                    break;
                                }
                            }
                        }
                        docker_referrers
                            .entry(subject_digest)
                            .or_default()
                            .push(ReferrerInfo {
                                digest: child_descriptor.digest.to_string(),
                                artifact_type: child_descriptor.artifact_type.clone(),
                                annotations,
                            });
                    }
                } else {
                    let platform = child_descriptor.platform.clone().map(Platform::from);
                    child_to_parents
                        .entry(child_descriptor.digest.clone())
                        .or_default()
                        .push((digest.clone(), platform));
                }
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
                for descriptor in oci_referrers {
                    referrers.push(ReferrerInfo {
                        digest: descriptor.digest.to_string(),
                        artifact_type: descriptor.artifact_type,
                        annotations: descriptor.annotations,
                    });
                }
            }

            let (pushed_at, last_pulled_at) = self
                .metadata_store
                .read_link(namespace, &LinkKind::Digest(digest.clone()), false)
                .await
                .map(|m| (m.created_at, m.accessed_at))
                .unwrap_or((None, None));

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
            self.blob_store
                .list_uploads(namespace, 1000, token)
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
            .filter(|ns| ns == repository || ns.starts_with(&format!("{repository}/")))
            .collect();

        matching_namespaces.sort_unstable();
        Ok(matching_namespaces)
    }
}
