//! The `scrub` command: a single categorizing walk over both stores.
//!
//! Every key is streamed, categorized by shape, and validated concurrently:
//! derivable state is repaired, unreadable content deleted, and unrecognized
//! keys quarantined (or deleted under `--delete-unknown`). Scrub is purely
//! structural; config-relative reclamation belongs to `angos prune`.

use std::sync::Arc;

use argh::FromArgs;
use tracing::info;

use super::orphan_jobs;

use crate::{
    command::{
        bootstrap,
        maintenance::{
            Error,
            executor::{ActionSink, DryRunSink, Executor, run_job_store},
            walk::{self, WalkStats},
        },
        reconcile,
        scrub::validate::{Pass, Validator},
    },
    configuration::Configuration,
    jobs::store::{ClaimMode, JobStore},
    registry::{
        Registry, blob_store::BlobStore, keys::REF_ROOT, metadata_store::MetadataStore,
        repository_resolver::RepositoryResolver,
    },
};

/// Default per-pass concurrency, shared by the scrub walk and the prune
/// sweeps.
pub fn default_concurrency() -> usize {
    25
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
    delete_unknown: bool,
    /// Held so the end of the run can drain in-flight async webhook
    /// deliveries the delete actions triggered.
    registry: Option<Arc<Registry>>,
    sink: Arc<dyn ActionSink>,
    repositories: Arc<RepositoryResolver>,
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
            // Tag and manifest deletions take the registry's standard delete
            // path (locking, blob reclaim, events, replication).
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
            sink.clone(),
            stats.clone(),
            options.delete_unknown,
        ));

        Ok(Self {
            blob_store,
            metadata_store,
            validator,
            stats,
            concurrency: options.concurrency,
            delete_unknown: options.delete_unknown,
            registry,
            sink,
            repositories,
        })
    }

    /// The passes, each internally concurrent. Ordering matters: links are
    /// healed and grants reconciled before index entries are pruned against
    /// them, so a listing failure aborts the run rather than letting a later
    /// pass delete on top of skipped repairs.
    pub async fn run(&mut self) -> Result<(), Error> {
        self.walk_pass(Pass::MetadataLinks, "").await?;
        self.walk_pass(Pass::MetadataReferences, REF_ROOT).await?;
        self.walk_pass(Pass::Blob, "").await?;
        // Listings and queued jobs are derived state the walk leaves alone;
        // which of them still resolve is decided against the configuration.
        reconcile::index::reclaim_unused_listings(
            self.blob_store.clone(),
            &self.metadata_store,
            self.repositories.clone(),
            self.sink.as_ref(),
        )
        .await?;
        orphan_jobs::sweep_orphan_jobs(
            &Arc::new(JobStore::new(
                self.metadata_store.object_store().clone(),
                "scrub-orphans",
                ClaimMode::Atomic,
            )),
            &self.repositories,
            self.sink.as_ref(),
            self.concurrency,
        )
        .await?;

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
            Pass::MetadataLinks | Pass::MetadataReferences => self.metadata_store.object_store(),
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

    use angos_oci::Namespace;

    use crate::command::scrub::command::*;
    use crate::{
        command::maintenance::action::LOST_AND_FOUND_PREFIX, registry::test_utils::seed_manifest,
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

    /// Full-command run over an FS root: junk is quarantined and a dry-run
    /// touches nothing.
    #[tokio::test]
    async fn command_run_quarantines_junk_and_reaches_a_fixpoint() {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path().to_string_lossy().to_string();
        let config = scrub_config(&root);
        let namespace = Namespace::new("test-repo/app").unwrap();

        // Seed content plus one defect through a throwaway command's stores.
        let seed = Command::new(&options(true, 2), &config).await.unwrap();
        seed_manifest(
            seed.metadata_store.object_store(),
            &seed.metadata_store,
            &namespace,
        )
        .await;
        let objects = seed.metadata_store.object_store();
        objects
            .put("stray/junk-object", Bytes::from_static(b"junk"))
            .await
            .unwrap();

        // Dry-run first: nothing changes.
        let mut dry = Command::new(&options(true, 2), &config).await.unwrap();
        dry.run().await.unwrap();
        assert!(objects.get("stray/junk-object").await.is_ok());

        // Real run: the junk is quarantined with its bytes preserved.
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

        // A repair can create new derivable state, so run to the fixpoint and
        // assert it is reached quickly.
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

    /// A tag entry's name carries the write's millisecond ordinal, which
    /// differs between the two seedings; normalise it before comparing.
    fn normalized(keys: &[String]) -> Vec<String> {
        keys.iter()
            .map(|key| match key.rsplit_once('/') {
                Some((dir, file)) if dir.contains("!tag/") && file.len() > 16 => {
                    format!("{dir}/ord{}", &file[16..])
                }
                _ => key.clone(),
            })
            .collect()
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
                seed.metadata_store.object_store(),
                &seed.metadata_store,
                &namespace,
            )
            .await;
            seed.metadata_store
                .object_store()
                .put("stray/junk", Bytes::from_static(b"junk"))
                .await
                .unwrap();

            let mut command = Command::new(&options(false, concurrency), &config)
                .await
                .unwrap();
            command.run().await.unwrap();

            let keys = std::sync::Mutex::new(Vec::new());
            walk::for_each_key(command.metadata_store.object_store(), "", 1, |key| {
                keys.lock().unwrap().push(key);
                async {}
            })
            .await
            .unwrap();
            roots.push((concurrency, keys.into_inner().unwrap(), temp_dir));
        }

        let (_, keys_serial, _dir_a) = &roots[0];
        let (_, keys_concurrent, _dir_b) = &roots[1];
        let (keys_serial, keys_concurrent) = (normalized(keys_serial), normalized(keys_concurrent));
        assert_eq!(
            keys_serial, keys_concurrent,
            "concurrency 1 and 8 must converge to the same key set"
        );
    }
}
