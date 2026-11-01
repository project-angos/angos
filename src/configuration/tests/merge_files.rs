use std::fs;

use tempfile::TempDir;

use crate::configuration::{Configuration, Error, ServerConfig};
use crate::registry::blob_store::BlobStoreConfig;
use crate::test_fixtures::configuration::{MINIMAL_CONFIG_TOML, config_toml};

/// Writes `documents` into a scratch directory and loads them as one merged
/// configuration, in the order given.
fn load_merged(documents: &[&str]) -> (TempDir, Result<Configuration, Error>) {
    let dir = TempDir::new().expect("scratch dir");
    let mut paths = Vec::with_capacity(documents.len());
    for (index, document) in documents.iter().enumerate() {
        let path = dir.path().join(format!("config-{index}.toml"));
        fs::write(&path, document).expect("write config");
        paths.push(path);
    }
    let result = Configuration::load_all(&paths);
    (dir, result)
}

/// The reason merging exists: an overlay can add `rules` to an access policy an
/// earlier file already opened, which a single concatenated document cannot do
/// because a repeated `[global.access_policy]` header is a TOML error.
#[test]
fn overlay_adds_rules_to_an_access_policy_the_base_opened() {
    let base = config_toml(
        r#"
        [global.access_policy]
        default = "deny"
    "#,
    );
    let overlay = r#"
        [global.access_policy]
        rules = ["request.action in ['healthz', 'readyz']"]
    "#;

    let (_dir, result) = load_merged(&[&base, overlay]);
    let config = result.expect("merged config loads");

    assert_eq!(config.global.access_policy.rules.len(), 1);
}

/// The same two documents concatenated are rejected, which is what an operator
/// hits when a chart appends raw TOML instead of passing a second file.
#[test]
fn concatenating_the_same_documents_is_a_duplicate_table_error() {
    let concatenated = format!(
        "{}\n{}",
        config_toml(
            r#"
            [global.access_policy]
            default = "deny"
        "#
        ),
        r#"
        [global.access_policy]
        rules = ["request.action in ['healthz']"]
    "#
    );

    let result = Configuration::load_from_str(&concatenated);

    assert!(
        matches!(result, Err(Error::InvalidFormat(_))),
        "duplicate table must be rejected, got {result:?}"
    );
}

#[test]
fn overlay_supplies_credentials_the_base_omits() {
    let base = r#"
        [server]
        bind_address = "0.0.0.0"

        [blob_store.s3]
        endpoint = "https://s3.example.com"
        bucket = "registry"
        region = "us-east-1"
        access_key_id = ""
        secret_key = ""
    "#;
    let overlay = r#"
        [blob_store.s3]
        access_key_id = "from-overlay"
        secret_key = "overlay-secret"
    "#;

    let (_dir, result) = load_merged(&[base, overlay]);
    let config = result.expect("merged config loads");

    let BlobStoreConfig::S3(s3) = &config.blob_store else {
        panic!("expected an S3 blob store");
    };
    assert_eq!(s3.connection.access_key_id.expose(), "from-overlay");
    assert_eq!(s3.connection.secret_key.expose(), "overlay-secret");
    assert_eq!(s3.connection.bucket, "registry");
}

#[test]
fn later_files_win_over_earlier_ones() {
    let overlay = r"
        [server]
        port = 9443
    ";

    let (_dir, result) = load_merged(&[MINIMAL_CONFIG_TOML, overlay]);
    let config = result.expect("merged config loads");

    let ServerConfig::Insecure(server) = config.server else {
        panic!("expected an insecure listener");
    };
    assert_eq!(server.port, 9443);
}

/// Validation runs on the merged whole, so a fragment that is not a
/// configuration on its own is still accepted as an overlay.
#[test]
fn a_fragment_alone_is_rejected_but_merges_cleanly() {
    let fragment = r"
        [global]
        update_pull_time = true
    ";

    let (_dir, alone) = load_merged(&[fragment]);
    assert!(
        matches!(alone, Err(Error::InvalidFormat(_))),
        "a fragment without [server] is not a configuration, got {alone:?}"
    );

    let (_dir, merged) = load_merged(&[MINIMAL_CONFIG_TOML, fragment]);
    assert!(merged.expect("merged config loads").global.update_pull_time);
}

#[test]
fn a_syntax_error_names_the_file_it_came_from() {
    let (_dir, result) = load_merged(&[MINIMAL_CONFIG_TOML, "this is not = = toml"]);

    let Err(Error::InvalidFormat(message)) = result else {
        panic!("expected a format error, got {result:?}");
    };
    assert!(
        message.contains("config-1.toml"),
        "must name the offending file: {message}"
    );
}

/// A merged tree has no single source to quote, so the error names the key path
/// and the files instead of a line and column.
#[test]
fn a_semantic_error_names_the_key_path_and_the_sources() {
    let overlay = r#"
        [server]
        port = "not-a-number"
    "#;

    let (_dir, result) = load_merged(&[MINIMAL_CONFIG_TOML, overlay]);

    let Err(Error::InvalidFormat(message)) = result else {
        panic!("expected a format error, got {result:?}");
    };
    assert!(
        message.contains("merged from"),
        "must list the source files: {message}"
    );
    assert!(
        message.contains("config-0.toml") && message.contains("config-1.toml"),
        "must name every source: {message}"
    );
}

/// Single-file loads keep the source excerpt, which merged loads cannot have.
#[test]
fn a_single_file_error_still_reports_line_and_column() {
    let invalid = r#"
        [blob_store.fs]
        root_dir = "/tmp/test"

        [server]
        bind_address = "0.0.0.0"
        port = "not-a-number"
    "#;

    let (_dir, result) = load_merged(&[invalid]);

    let Err(Error::InvalidFormat(message)) = result else {
        panic!("expected a format error, got {result:?}");
    };
    assert!(
        message.contains("line") && message.contains("column"),
        "single-file errors keep their span: {message}"
    );
}

#[test]
fn loading_no_files_at_all_is_an_error() {
    let empty: [&str; 0] = [];
    let result = Configuration::load_all(&empty);

    assert!(
        matches!(result, Err(Error::NotReadable(_))),
        "expected a NotReadable error, got {result:?}"
    );
}
