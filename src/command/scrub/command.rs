//! The `scrub` command: a single categorizing walk over both stores.
//!
//! Every object key is streamed, categorized by shape, and validated
//! concurrently: derivable state is repaired, unreadable content is deleted,
//! and keys matching no known angos layout are quarantined under the
//! lost-and-found prefix (or deleted outright with `--delete-unknown`).
//! Scrub is purely structural; age-gated reclamation (upload sessions,
//! orphan grants) belongs to `angos prune`.

use std::sync::Arc;

use argh::FromArgs;
use tracing::info;

use crate::{
    command::{
        bootstrap,
        scrub::{
            error::Error,
            executor::{ActionSink, DryRunSink, Executor, run_job_store},
            validate::{Pass, Validator},
            walk::{self, WalkStats},
        },
    },
    configuration::Configuration,
    registry::{Registry, blob_store::BlobStore, metadata_store::MetadataStore, path_builder},
};

/// Default per-pass concurrency, shared by the scrub walk and the prune
/// sweeps.
pub fn default_concurrency() -> usize {
    8
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(
    subcommand,
    name = "scrub",
    description = "Walk the store, repair inconsistencies, and quarantine unrecognized objects"
)]
pub struct Options {
    #[argh(switch, short = 'd')]
    /// display only, no actual changes applied
    pub dry_run: bool,
    #[argh(option, default = "default_concurrency()")]
    /// number of keys validated concurrently per walk pass
    pub concurrency: usize,
    #[argh(switch)]
    /// delete unrecognized keys outright instead of quarantining them under
    /// the lost-and-found prefix
    pub delete_unknown: bool,
}

pub struct Command {
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
    validator: Arc<Validator>,
    stats: Arc<WalkStats>,
    concurrency: usize,
    dry_run: bool,
    delete_unknown: bool,
    /// Held so the end of the run can drain in-flight async webhook
    /// deliveries the delete actions triggered.
    registry: Option<Arc<Registry>>,
}

impl Command {
    pub async fn new(options: &Options, config: &Configuration) -> Result<Self, Error> {
        let bootstrap::MaintenanceContext {
            blob_store,
            metadata_store,
            repositories,
        } = bootstrap::maintenance_context(config).await?;

        let mut registry = None;
        let sink: Arc<dyn ActionSink> = if options.dry_run {
            info!("Dry-run mode: no changes will be made to the storage");
            Arc::new(DryRunSink)
        } else {
            let job_store = run_job_store(&metadata_store, "scrub");
            // Tag and manifest deletions go through the registry's standard
            // delete path (locking, blob reclaim, events, replication), so
            // scrub always wires one.
            let scrub_registry = bootstrap::registry(
                config,
                blob_store.clone(),
                metadata_store.clone(),
                repositories.clone(),
                job_store.clone(),
            )?;
            registry = Some(scrub_registry.clone());
            Arc::new(
                Executor::new(blob_store.clone(), metadata_store.clone(), job_store)
                    .with_registry(scrub_registry),
            )
        };

        let stats = Arc::new(WalkStats::default());
        let validator = Arc::new(Validator::new(
            blob_store.clone(),
            metadata_store.clone(),
            sink,
            stats.clone(),
            options.delete_unknown,
        ));

        Ok(Self {
            blob_store,
            metadata_store,
            validator,
            stats,
            concurrency: options.concurrency,
            dry_run: options.dry_run,
            delete_unknown: options.delete_unknown,
            registry,
        })
    }

    /// The three passes, each internally concurrent. Ordering matters: links
    /// are healed and grants reconciled before shard entries are pruned
    /// against them, and the index is healed before blob GC reads it. A
    /// listing failure aborts the run, since the later passes' deletions rely
    /// on the earlier repairs.
    pub async fn run(&mut self) -> Result<(), Error> {
        // Engine housekeeping first: reclaim orphaned transaction staging
        // bodies and expired lock objects through the engine's own janitors
        // (age-gated by engine thresholds; serving processes no longer run
        // periodic janitor loops). The sweep mutates directly, so dry-run
        // skips it.
        if self.dry_run {
            info!("Dry-run mode: skipping the engine janitor sweep");
        } else {
            self.metadata_store.store().janitor_sweep().await;
        }

        self.walk_pass(Pass::MetadataLinks, "").await?;
        self.walk_pass(Pass::MetadataShards, path_builder::blobs_root_dir())
            .await?;
        self.walk_pass(Pass::Blob, "").await?;

        self.metadata_store.flush_access_times().await;
        if let Some(registry) = &self.registry {
            registry.shutdown().await;
        }
        info!(
            "scrub complete: {}",
            self.stats.summary(self.delete_unknown)
        );
        Ok(())
    }

    async fn walk_pass(&self, pass: Pass, prefix: &str) -> Result<(), Error> {
        let objects = match pass {
            Pass::MetadataLinks | Pass::MetadataShards => {
                self.metadata_store.store().object_store()
            }
            Pass::Blob => self.blob_store.object_store(),
        };
        let validator = &self.validator;
        walk::for_each_key(objects, prefix, self.concurrency, |key| async move {
            validator.process(pass, &key).await;
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tempfile::TempDir;

    use super::*;
    use crate::{
        command::scrub::action::LOST_AND_FOUND_PREFIX,
        oci::{Digest, Namespace},
        registry::{
            metadata_store::LinkMetadata, path_builder as paths, test_utils::seed_manifest,
        },
    };

    fn scrub_config(root: &str) -> Configuration {
        let toml = format!(
            r#"
            [blob_store.fs]
            root_dir = "{root}"

            [metadata_store.fs]
            root_dir = "{root}"

            [cache.memory]

            [server]
            bind_address = "0.0.0.0"
            port = 8000

            [global]
            update_pull_time = false

            [global.retention_policy]
            rules = []

            [repository."test-repo"]
            "#
        );
        Configuration::load_from_str(&toml).unwrap()
    }

    fn options(dry_run: bool, concurrency: usize) -> Options {
        Options {
            dry_run,
            concurrency,
            delete_unknown: false,
        }
    }

    /// Full-command run over an FS root: junk is quarantined, an invalid tag
    /// directory is removed, and a dry-run touches nothing.
    #[tokio::test]
    async fn command_run_quarantines_junk_and_removes_invalid_tags() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path().to_string_lossy().to_string();
        let config = scrub_config(&root);
        let namespace = Namespace::new("test-repo/app").unwrap();

        // Seed content plus two defects through a throwaway command's stores.
        let seed = Command::new(&options(true, 2), &config).await.unwrap();
        seed_manifest(
            seed.metadata_store.store(),
            &seed.metadata_store,
            &namespace,
        )
        .await;
        let objects = seed.metadata_store.store().object_store();
        objects
            .put("stray/junk-object", Bytes::from_static(b"junk"))
            .await
            .unwrap();
        let bad_tag_key = format!(
            "{}/current/link",
            paths::manifest_tag_dir(&namespace, "-bad")
        );
        let bad_body =
            serde_json::to_vec(&LinkMetadata::from_digest(Digest::sha256_of_bytes(b"x"))).unwrap();
        objects
            .put(&bad_tag_key, Bytes::from(bad_body))
            .await
            .unwrap();

        // Dry-run first: nothing changes.
        let mut dry = Command::new(&options(true, 2), &config).await.unwrap();
        dry.run().await.unwrap();
        assert!(objects.get("stray/junk-object").await.is_ok());
        assert!(objects.get(&bad_tag_key).await.is_ok());

        // Real run: junk quarantined, invalid tag directory removed.
        let mut real = Command::new(&options(false, 4), &config).await.unwrap();
        real.run().await.unwrap();
        assert!(objects.get("stray/junk-object").await.is_err());
        assert_eq!(
            objects
                .get(&format!("{LOST_AND_FOUND_PREFIX}/stray/junk-object"))
                .await
                .unwrap(),
            b"junk"
        );
        assert!(objects.get(&bad_tag_key).await.is_err());

        // Convergence: a repair can create new derivable state (the recreated
        // revision link derives back-links on the next pass), so run to the
        // fixpoint and assert it is reached quickly.
        let mut runs = 0;
        loop {
            let mut again = Command::new(&options(false, 4), &config).await.unwrap();
            again.run().await.unwrap();
            let stats = &again.stats;
            let emitted = stats.repairs.load(std::sync::atomic::Ordering::Relaxed)
                + stats.quarantined.load(std::sync::atomic::Ordering::Relaxed)
                + stats.corrupt.load(std::sync::atomic::Ordering::Relaxed);
            if emitted == 0 {
                break;
            }
            runs += 1;
            assert!(
                runs < 4,
                "the store must converge within a few runs (still emitting {emitted})"
            );
        }
    }

    /// The same seeded defects converge to the same state regardless of the
    /// concurrency level.
    #[tokio::test]
    async fn concurrency_level_does_not_change_the_outcome() {
        let mut roots = Vec::new();
        for concurrency in [1, 8] {
            let temp_dir = TempDir::new().unwrap();
            let root = temp_dir.path().to_string_lossy().to_string();
            let config = scrub_config(&root);
            let namespace = Namespace::new("test-repo/app").unwrap();

            let seed = Command::new(&options(true, 1), &config).await.unwrap();
            seed_manifest(
                seed.metadata_store.store(),
                &seed.metadata_store,
                &namespace,
            )
            .await;
            seed.metadata_store
                .store()
                .object_store()
                .put("stray/junk", Bytes::from_static(b"junk"))
                .await
                .unwrap();

            let mut command = Command::new(&options(false, concurrency), &config)
                .await
                .unwrap();
            command.run().await.unwrap();

            // Collect the final key set (sorted by the walker).
            let keys = std::sync::Mutex::new(Vec::new());
            walk::for_each_key(
                command.metadata_store.store().object_store(),
                "",
                1,
                |key| {
                    keys.lock().unwrap().push(key);
                    async {}
                },
            )
            .await
            .unwrap();
            roots.push((concurrency, keys.into_inner().unwrap(), temp_dir));
        }

        let (_, keys_serial, _dir_a) = &roots[0];
        let (_, keys_concurrent, _dir_b) = &roots[1];
        assert_eq!(
            keys_serial, keys_concurrent,
            "concurrency 1 and 8 must converge to the same key set"
        );
    }
}
