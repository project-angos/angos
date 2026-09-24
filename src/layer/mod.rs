//! Image filesystem exploring. A layer is a tar stream, usually gzipped, with
//! no random access; [`IndexLayerJobHandler`] walks it once and keeps, by the
//! layer digest, a listing of its entries with their offsets and the
//! inflater's checkpoints, so the web UI can browse the merged filesystem and
//! open a file without decoding the whole layer again. A push of an image
//! a repository's index policy applies to enqueues one [`INDEX_LAYER_KIND`]
//! job per layer; any other image is indexed the first time someone asks.

use std::{
    ffi::OsStr,
    io::{self, BufRead, BufReader, Read, Write},
    mem,
    path::Path,
    sync::Arc,
};

use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256, Sha512};
use tokio_util::io::SyncIoBridge;
use tracing::{debug, info};

use angos_oci::{Content, Digest, Manifest};

use crate::{
    jobs::{
        Queue,
        store::{Error, JobEnvelope, JobHandler},
    },
    registry::{
        Error as RegistryError, blob_store::BlobStore, keys::DigestKeys,
        metadata_store::MetadataStore,
    },
};

pub use angos_inflate::{Checkpoint, Inflater};

pub mod elf;
pub mod pem;

pub const INDEX_LAYER_KIND: &str = "index.layer";

/// What an `index` table's `default` gives an image no rule matches.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IndexAction {
    Index,
    Skip,
}

impl From<IndexAction> for bool {
    fn from(action: IndexAction) -> bool {
        matches!(action, IndexAction::Index)
    }
}
/// Output between two checkpoints: what opening a file costs at most in
/// decoding, against 32 KiB of stored window per checkpoint.
const CHECKPOINT_EVERY: u64 = 4 * 1024 * 1024;
const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];
const ELF_MAGIC: [u8; 4] = [0x7f, b'E', b'L', b'F'];
/// What indexing records; bump it when that changes, so older listings are
/// walked again.
const LISTING_VERSION: u32 = 3;
/// A file's first bytes, where a NUL makes it binary, as the web UI judges.
const SNIFF_LEN: usize = 8192;
/// What is scanned of a line for secrets; the rest of a longer one is not.
const LINE_LIMIT: usize = 64 * 1024;
const KEY_MARKER: &[u8] = b"PRIVATE KEY-----";
const CAPABILITY_XATTR: &[u8] = b"SCHILY.xattr.security.capability";
/// Linux capabilities by bit, as `getcap` names them.
const CAPABILITIES: [&str; 41] = [
    "cap_chown",
    "cap_dac_override",
    "cap_dac_read_search",
    "cap_fowner",
    "cap_fsetid",
    "cap_kill",
    "cap_setgid",
    "cap_setuid",
    "cap_setpcap",
    "cap_linux_immutable",
    "cap_net_bind_service",
    "cap_net_broadcast",
    "cap_net_admin",
    "cap_net_raw",
    "cap_ipc_lock",
    "cap_ipc_owner",
    "cap_sys_module",
    "cap_sys_rawio",
    "cap_sys_chroot",
    "cap_sys_ptrace",
    "cap_sys_pacct",
    "cap_sys_admin",
    "cap_sys_boot",
    "cap_sys_nice",
    "cap_sys_resource",
    "cap_sys_time",
    "cap_sys_tty_config",
    "cap_mknod",
    "cap_lease",
    "cap_audit_write",
    "cap_audit_control",
    "cap_setfcap",
    "cap_mac_override",
    "cap_mac_admin",
    "cap_syslog",
    "cap_wake_alarm",
    "cap_block_suspend",
    "cap_audit_read",
    "cap_perfmon",
    "cap_bpf",
    "cap_checkpoint_restore",
];

/// A service's token: its prefix, then at least `least` characters `body` admits.
struct TokenShape {
    prefix: &'static [u8],
    least: usize,
    body: fn(u8) -> bool,
    kind: SecretKind,
}

const fn token(
    prefix: &'static [u8],
    least: usize,
    body: fn(u8) -> bool,
    kind: SecretKind,
) -> TokenShape {
    TokenShape {
        prefix,
        least,
        body,
        kind,
    }
}

const TOKENS: [TokenShape; 13] = [
    token(b"ghp_", 36, is_alphanumeric, SecretKind::GithubToken),
    token(b"gho_", 36, is_alphanumeric, SecretKind::GithubToken),
    token(b"ghu_", 36, is_alphanumeric, SecretKind::GithubToken),
    token(b"ghs_", 36, is_alphanumeric, SecretKind::GithubToken),
    token(b"ghr_", 36, is_alphanumeric, SecretKind::GithubToken),
    token(b"github_pat_", 60, is_token, SecretKind::GithubToken),
    token(b"glpat-", 20, is_token, SecretKind::GitlabToken),
    token(b"xoxb-", 20, is_token, SecretKind::SlackToken),
    token(b"xoxp-", 20, is_token, SecretKind::SlackToken),
    token(b"sk_live_", 24, is_alphanumeric, SecretKind::StripeKey),
    token(b"rk_live_", 24, is_alphanumeric, SecretKind::StripeKey),
    token(b"AKIA", 16, is_key_id, SecretKind::AwsAccessKey),
    token(b"ASIA", 16, is_key_id, SecretKind::AwsAccessKey),
];
/// Source extensions `mime_guess` misses or leaves at `text/plain`.
const SOURCE_TYPES: [(&str, &str); 13] = [
    ("bash", "application/x-sh"),
    ("c", "text/x-c"),
    ("cc", "text/x-c++"),
    ("cpp", "text/x-c++"),
    ("cxx", "text/x-c++"),
    ("go", "text/x-go"),
    ("h", "text/x-c"),
    ("hh", "text/x-c++"),
    ("hpp", "text/x-c++"),
    ("java", "text/x-java"),
    ("py", "text/x-python"),
    ("rb", "text/x-ruby"),
    ("ts", "application/typescript"),
];
/// `#!` interpreters, version suffix stripped, for scripts named without an
/// extension.
const INTERPRETERS: [(&str, &str); 12] = [
    ("ash", "application/x-sh"),
    ("bash", "application/x-sh"),
    ("dash", "application/x-sh"),
    ("ksh", "application/x-sh"),
    ("lua", "text/x-lua"),
    ("node", "text/javascript"),
    ("perl", "application/x-perl"),
    ("php", "application/x-httpd-php"),
    ("python", "text/x-python"),
    ("ruby", "text/x-ruby"),
    ("sh", "application/x-sh"),
    ("zsh", "application/x-sh"),
];

/// JSON payload of an [`INDEX_LAYER_KIND`] job. The namespace only names who
/// asked: the listing is keyed by the digest and shared like the blob is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexLayerPayload {
    pub namespace: angos_oci::Namespace,
    pub digest: Digest,
    /// Walk the layer again even when a listing exists.
    #[serde(default)]
    pub force: bool,
}

/// An index job keyed on `index.{digest}`, so every image sharing the layer
/// coalesces on one job.
pub fn build_envelope(payload: &IndexLayerPayload) -> Result<JobEnvelope, Error> {
    JobEnvelope::new(
        Queue::Index,
        INDEX_LAYER_KIND,
        format!("{}.{}", Queue::Index, payload.digest),
        payload,
    )
}

/// Whether a layer media type names a tar stream the indexer can walk: plain
/// or gzipped, in the OCI or Docker spelling. zstd layers are not.
pub fn is_filesystem_layer(media_type: &str) -> bool {
    media_type.contains(".tar") && !media_type.contains("zstd")
}

/// The digests of a plain image manifest's walkable layers; empty for an
/// index, a referrer or an artifact.
pub fn filesystem_layers(manifest: &Manifest) -> Vec<Digest> {
    if manifest.subject.is_some() || manifest.artifact_type.is_some() {
        return Vec::new();
    }
    match &manifest.content {
        Content::Image { layers, .. } => layers
            .iter()
            .filter(|layer| is_filesystem_layer(layer.media_type.as_ref()))
            .map(|layer| layer.digest.clone())
            .collect(),
        Content::Index { .. } => Vec::new(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    File,
    Dir,
    Symlink,
    Hardlink,
    /// `.wh.<name>`: the lower layers' `<name>` is gone; `path` names it.
    Whiteout,
    /// `.wh..wh..opq`: the lower layers' content of `path` is gone.
    Opaque,
    Other,
}

/// One tar entry, with where its data starts in the uncompressed stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub path: String,
    pub kind: Kind,
    pub size: u64,
    pub mode: u32,
    pub uid: u64,
    pub gid: u64,
    pub mtime: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link: Option<String>,
    pub offset: u64,
    /// Set on files only, and missing from listings indexed before it existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<FileContent>,
    /// The Linux capabilities its `security.capability` attribute permits.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
}

/// A file's digests, hex-encoded, its media type, and the credentials its
/// first bytes give away.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileContent {
    pub sha256: String,
    pub sha512: String,
    pub mime_type: String,
    /// In line order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secrets: Vec<Secret>,
}

/// A credential, and the line it is on from 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Secret {
    pub kind: SecretKind,
    pub line: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SecretKind {
    /// A PEM, OpenSSH or PGP private key block.
    PrivateKey,
    /// An `aws_secret_access_key` line of an AWS credentials file.
    AwsCredentials,
    /// An `auth` entry of a Docker `config.json`.
    RegistryAuth,
    /// An `.npmrc` registry token or password.
    NpmToken,
    /// A URL carrying a user and a password, as `.git-credentials` keeps them.
    GitCredentials,
    /// A `.netrc` machine with its password.
    Netrc,
    /// A GitHub personal access, OAuth or app token.
    GithubToken,
    /// A GitLab personal access token.
    GitlabToken,
    /// A Slack bot or user token.
    SlackToken,
    /// A Stripe live secret or restricted key.
    StripeKey,
    /// An AWS access key ID, long-lived or temporary.
    AwsAccessKey,
    /// A kubeconfig's client key or bearer token.
    Kubeconfig,
}

/// Digests a file's bytes as they stream past, keeping the first ones and
/// scanning them for secrets.
struct Fingerprint {
    sha256: Sha256,
    sha512: Sha512,
    head: Vec<u8>,
    secrets: SecretScanner,
}

impl Write for Fingerprint {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let room = SNIFF_LEN.saturating_sub(self.head.len()).min(buf.len());
        self.head.extend_from_slice(&buf[..room]);
        self.sha256.update(buf);
        self.sha512.update(buf);
        self.secrets.feed(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Reads a file to its end into its [`FileContent`].
fn inspect(path: &str, mut data: impl Read) -> io::Result<FileContent> {
    let mut fingerprint = Fingerprint {
        sha256: Sha256::new(),
        sha512: Sha512::new(),
        head: Vec::with_capacity(SNIFF_LEN),
        secrets: SecretScanner::new(path),
    };
    io::copy(&mut data, &mut fingerprint)?;
    Ok(FileContent {
        sha256: hex::encode(fingerprint.sha256.finalize()),
        sha512: hex::encode(fingerprint.sha512.finalize()),
        mime_type: mime_type(path, &fingerprint.head),
        secrets: fingerprint.secrets.finish(),
    })
}

/// ELF or PEM content first, then the extension, then a `#!` line, then text
/// unless `head` holds a NUL.
fn mime_type(path: &str, head: &[u8]) -> String {
    if head.starts_with(&ELF_MAGIC) {
        return "application/x-executable".to_string();
    }
    // A key named `.pem` or `.crt` is no CA certificate, which the extension would say.
    if head.trim_ascii_start().starts_with(b"-----BEGIN ") {
        return "application/x-pem-file".to_string();
    }
    let extension = Path::new(path)
        .extension()
        .and_then(OsStr::to_str)
        .unwrap_or_default();
    SOURCE_TYPES
        .iter()
        .find(|(known, _)| known.eq_ignore_ascii_case(extension))
        .map(|(_, mime)| (*mime).to_string())
        .or_else(|| {
            mime_guess::from_ext(extension)
                .first()
                .map(|mime| mime.to_string())
        })
        .or_else(|| script_type(head).map(str::to_string))
        .unwrap_or_else(|| {
            if head.contains(&0) {
                "application/octet-stream".to_string()
            } else {
                "text/plain".to_string()
            }
        })
}

/// The media type of a `#!` line's interpreter, looked up past `env` and a
/// version suffix such as `python3.12`'s.
fn script_type(head: &[u8]) -> Option<&'static str> {
    let line = head
        .strip_prefix(b"#!")?
        .split(|&byte| byte == b'\n')
        .next()?;
    let mut words = str::from_utf8(line).ok()?.split_ascii_whitespace();
    let mut program = words.next()?.rsplit('/').next()?;
    if program == "env" {
        program = words.find(|word| !word.starts_with('-'))?;
    }
    let program = program.trim_end_matches(|c: char| c.is_ascii_digit() || c == '.');
    INTERPRETERS
        .iter()
        .find(|(name, _)| *name == program)
        .map(|(_, mime)| *mime)
}

/// Finds a text file's credentials line by line as its bytes stream past; a
/// file whose first [`SNIFF_LEN`] bytes hold a NUL is binary and has none.
struct SecretScanner {
    /// The login shape the file's name admits: documentation quotes those lines.
    login: Option<SecretKind>,
    kubeconfig: bool,
    seen: usize,
    binary: bool,
    line: Vec<u8>,
    number: usize,
    /// An `"auths"` came first, making an `"auth"` a registry login.
    registry: bool,
    /// The line opening a private key block, whose base64 the next one starts with.
    key_opened: Option<usize>,
    found: Vec<Secret>,
}

impl SecretScanner {
    fn new(path: &str) -> Self {
        let name = Path::new(path)
            .file_name()
            .and_then(OsStr::to_str)
            .unwrap_or_default();
        SecretScanner {
            login: match name {
                ".npmrc" | "npmrc" => Some(SecretKind::NpmToken),
                ".git-credentials" => Some(SecretKind::GitCredentials),
                ".netrc" | "_netrc" => Some(SecretKind::Netrc),
                _ => None,
            },
            kubeconfig: name == "kubeconfig"
                || name.ends_with(".kubeconfig")
                || path.ends_with(".kube/config"),
            seen: 0,
            binary: false,
            line: Vec::new(),
            number: 0,
            registry: false,
            key_opened: None,
            found: Vec::new(),
        }
    }

    fn feed(&mut self, mut bytes: &[u8]) {
        let sniffed = SNIFF_LEN.saturating_sub(self.seen).min(bytes.len());
        self.binary |= bytes[..sniffed].contains(&0);
        self.seen = self.seen.saturating_add(bytes.len());
        if self.binary {
            return;
        }
        while let Some(end) = bytes.iter().position(|&byte| byte == b'\n') {
            self.push(&bytes[..end]);
            self.end_line();
            bytes = &bytes[end + 1..];
        }
        self.push(bytes);
    }

    fn push(&mut self, bytes: &[u8]) {
        let room = LINE_LIMIT.saturating_sub(self.line.len()).min(bytes.len());
        self.line.extend_from_slice(&bytes[..room]);
    }

    fn end_line(&mut self) {
        self.number += 1;
        let mut line = mem::take(&mut self.line);
        self.scan(line.strip_suffix(b"\r").unwrap_or(&line));
        line.clear();
        self.line = line;
    }

    /// The credentials found, in line order.
    fn finish(mut self) -> Vec<Secret> {
        if self.binary {
            return Vec::new();
        }
        if !self.line.is_empty() {
            self.end_line();
        }
        self.found
    }

    fn scan(&mut self, line: &[u8]) {
        // A PEM block's base64 opens the line after its `BEGIN` line.
        if let Some(opened) = self.key_opened.take()
            && base64_run(line) >= 16
        {
            self.found.push(Secret {
                kind: SecretKind::PrivateKey,
                line: opened,
            });
        }
        let mut kinds = Vec::new();
        for at in positions(line, KEY_MARKER) {
            // A block opens its line, unless it is a GCP service account's JSON value.
            let before = line[..at].trim_ascii_start();
            if !before.starts_with(b"-----BEGIN ") && find(before, b"\"private_key\"").is_none() {
                continue;
            }
            let rest = &line[at + KEY_MARKER.len()..];
            if rest.is_empty() {
                self.key_opened = Some(self.number);
            } else if rest
                .strip_prefix(b"\\n")
                .is_some_and(|body| base64_run(body) >= 16)
            {
                kinds.push(SecretKind::PrivateKey);
            }
        }
        self.registry |= find(line, b"\"auths\"").is_some();
        if self.registry && registry_login(line) {
            kinds.push(SecretKind::RegistryAuth);
        }
        let login = match self.login {
            Some(SecretKind::NpmToken) => npm_token(line),
            Some(SecretKind::GitCredentials) => credential_url(line),
            Some(SecretKind::Netrc) => find(line, b"password ").is_some(),
            _ => false,
        };
        if login {
            kinds.extend(self.login);
        }
        if aws_secret(line) {
            kinds.push(SecretKind::AwsCredentials);
        }
        if kubeconfig_key(line) || (self.kubeconfig && kubeconfig_token(line)) {
            kinds.push(SecretKind::Kubeconfig);
        }
        kinds.extend(tokens(line));
        kinds.sort_unstable();
        kinds.dedup();
        let number = self.number;
        self.found
            .extend(kinds.into_iter().map(|kind| Secret { kind, line: number }));
    }
}

/// The service tokens of `line`: each a run of token characters opening with
/// a known prefix, long enough, and mixing letters and digits the way a random
/// one does, where documentation shows `ghp_xxxx…` or AWS's `…EXAMPLE`.
fn tokens(line: &[u8]) -> impl Iterator<Item = SecretKind> + '_ {
    line.split(|&byte| !is_token(byte))
        .filter(|run| run.len() >= 20)
        .filter_map(|run| {
            let shape = TOKENS.iter().find(|shape| run.starts_with(shape.prefix))?;
            let body = run.get(shape.prefix.len()..shape.prefix.len() + shape.least)?;
            let random = body.iter().copied().all(shape.body)
                && body.iter().any(u8::is_ascii_digit)
                && body.iter().any(u8::is_ascii_alphabetic)
                && find(run, b"EXAMPLE").is_none();
            random.then_some(shape.kind)
        })
}

fn is_alphanumeric(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
}

fn is_token(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')
}

fn is_key_id(byte: u8) -> bool {
    byte.is_ascii_uppercase() || byte.is_ascii_digit()
}

/// A kubeconfig user's `client-key-data`, set to base64 rather than `REDACTED`.
fn kubeconfig_key(line: &[u8]) -> bool {
    line.trim_ascii_start()
        .strip_prefix(b"client-key-data:")
        .is_some_and(|value| base64_run(value.trim_ascii_start()) >= 16)
}

/// A kubeconfig user's bearer `token`, set to a value rather than a placeholder.
fn kubeconfig_token(line: &[u8]) -> bool {
    line.trim_ascii_start()
        .strip_prefix(b"token:")
        .is_some_and(|value| {
            let value = value.trim_ascii_start();
            let value = value
                .strip_prefix(b"\"")
                .or_else(|| value.strip_prefix(b"'"))
                .unwrap_or(value);
            value.iter().take_while(|&&byte| is_token(byte)).count() >= 16
        })
}

/// A credentials file's secret key: 40 characters, where documentation shows
/// a placeholder or AWS's `…EXAMPLEKEY`.
fn aws_secret(line: &[u8]) -> bool {
    line.strip_prefix(b"aws_secret_access_key")
        .and_then(|rest| rest.trim_ascii_start().strip_prefix(b"="))
        .map(<[u8]>::trim_ascii)
        .is_some_and(|value| {
            value.len() == 40
                && value.iter().copied().all(is_base64)
                && find(value, b"EXAMPLE").is_none()
        })
}

fn is_base64(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'=')
}

/// How many base64 characters `bytes` opens with.
fn base64_run(bytes: &[u8]) -> usize {
    bytes.iter().take_while(|&&byte| is_base64(byte)).count()
}

/// Whether an `"auth"` of a Docker `config.json` is set to a value on `line`.
fn registry_login(line: &[u8]) -> bool {
    const KEY: &[u8] = b"\"auth\"";
    positions(line, KEY).any(|at| {
        line[at + KEY.len()..]
            .trim_ascii_start()
            .strip_prefix(b":")
            .and_then(|value| value.trim_ascii_start().strip_prefix(b"\""))
            .and_then(|value| value.first())
            .is_some_and(|&byte| byte != b'"')
    })
}

/// An `.npmrc` login, `_auth`, `_authToken` or a registry's `:_password`, set
/// to a value rather than to an `${ENV}` reference.
fn npm_token(line: &[u8]) -> bool {
    let Some((key, value)) = str::from_utf8(line)
        .ok()
        .and_then(|line| line.split_once('='))
    else {
        return false;
    };
    let (key, value) = (key.trim(), value.trim());
    let login = matches!(key, "_auth" | "_authToken")
        || [":_auth", ":_authToken", ":_password"]
            .iter()
            .any(|suffix| key.ends_with(suffix));
    login && !value.is_empty() && !value.starts_with("${")
}

/// A line that is a URL with a user and a password.
fn credential_url(line: &[u8]) -> bool {
    let Ok(line) = str::from_utf8(line.trim_ascii()) else {
        return false;
    };
    let Some(rest) = line
        .strip_prefix("https://")
        .or_else(|| line.strip_prefix("http://"))
    else {
        return false;
    };
    let Some((user, rest)) = rest.split_once(':') else {
        return false;
    };
    let Some((password, host)) = rest.split_once('@') else {
        return false;
    };
    [user, password, host].iter().all(|part| !part.is_empty())
        && !user.contains('/')
        && !password.contains('/')
        && !line.contains(char::is_whitespace)
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    positions(haystack, needle).next()
}

/// Where `needle` starts in `haystack`, every time.
fn positions<'a>(haystack: &'a [u8], needle: &'a [u8]) -> impl Iterator<Item = usize> + 'a {
    haystack
        .windows(needle.len())
        .enumerate()
        .filter(move |&(_, window)| window == needle)
        .map(|(at, _)| at)
}

/// The capabilities a tar entry's `security.capability` attribute permits, as
/// its PAX header carries it.
fn entry_capabilities<R: Read>(entry: &mut tar::Entry<'_, R>) -> io::Result<Vec<String>> {
    let Some(extensions) = entry.pax_extensions()? else {
        return Ok(Vec::new());
    };
    for extension in extensions {
        let extension = extension?;
        if extension.key_bytes() == CAPABILITY_XATTR {
            return Ok(capabilities(extension.value_bytes()));
        }
    }
    Ok(Vec::new())
}

/// The permitted set of a `vfs_cap_data` value, by name: its low 32 bits at
/// offset 4, the high ones at 12 from revision 2 on.
fn capabilities(value: &[u8]) -> Vec<String> {
    let word = |at: usize| {
        value
            .get(at..at + 4)
            .and_then(|bytes| bytes.try_into().ok())
            .map_or(0, u32::from_le_bytes)
    };
    let permitted = u64::from(word(4)) | u64::from(word(12)) << 32;
    (0..64)
        .filter(|bit| permitted >> bit & 1 == 1)
        .map(|bit| {
            CAPABILITIES
                .get(bit)
                .map_or_else(|| format!("cap_{bit}"), |name| (*name).to_string())
        })
        .collect()
}

/// A layer's entries in tar order, stored as JSON by the layer digest and
/// served as-is to the web UI.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Listing {
    /// The [`LISTING_VERSION`] that wrote it, 0 before there was one.
    #[serde(default)]
    pub version: u32,
    /// Whether the layer is gzipped, which is when checkpoints exist.
    pub compressed: bool,
    pub uncompressed_size: u64,
    pub entries: Vec<Entry>,
}

impl Listing {
    /// Whether an older version wrote it.
    pub fn is_outdated(&self) -> bool {
        self.version < LISTING_VERSION
    }
}

/// The inflater's checkpoints, stored next to the listing with each window
/// base64-encoded.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Checkpoints {
    pub items: Vec<StoredCheckpoint>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredCheckpoint {
    pub in_offset: u64,
    pub bit: u8,
    pub out_offset: u64,
    pub window: String,
}

impl Checkpoints {
    pub fn from_inflater(checkpoints: Vec<Checkpoint>) -> Self {
        Self {
            items: checkpoints
                .into_iter()
                .map(|checkpoint| StoredCheckpoint {
                    in_offset: checkpoint.in_offset,
                    bit: checkpoint.bit,
                    out_offset: checkpoint.out_offset,
                    window: STANDARD.encode(checkpoint.window),
                })
                .collect(),
        }
    }

    /// The last checkpoint at or before `offset`, the one a read there resumes
    /// from; `None` means decoding from the start.
    pub fn before(&self, offset: u64) -> Option<Checkpoint> {
        self.items
            .iter()
            .filter(|checkpoint| checkpoint.out_offset <= offset)
            .max_by_key(|checkpoint| checkpoint.out_offset)
            .and_then(|checkpoint| {
                Some(Checkpoint {
                    in_offset: checkpoint.in_offset,
                    bit: checkpoint.bit,
                    out_offset: checkpoint.out_offset,
                    window: STANDARD.decode(&checkpoint.window).ok()?,
                })
            })
    }
}

/// The tar stream behind a layer: inflated, or the bytes as they are.
enum Source<R: Read> {
    Gzip(Box<Inflater<BufReader<R>>>),
    Plain(BufReader<R>, u64),
}

impl<R: Read> Read for Source<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Source::Gzip(inflater) => inflater.read(buf),
            Source::Plain(reader, count) => {
                let n = reader.read(buf)?;
                *count += n as u64;
                Ok(n)
            }
        }
    }
}

/// `usr/bin/.wh.sh` is a whiteout of `usr/bin/sh`, `a/.wh..wh..opq` the
/// opaque marker of `a`; the rest keep their kind. Paths lose the `./` and
/// trailing `/` tar spellings so the same file is named the same in every
/// layer.
fn classify(raw: &str, tar_kind: tar::EntryType) -> Option<(String, Kind)> {
    let path = raw.trim_start_matches("./").trim_matches('/');
    if path.is_empty() || path == "." {
        return None;
    }
    let (dir, name) = path.rsplit_once('/').unwrap_or(("", path));
    let join = |name: &str| {
        if dir.is_empty() {
            name.to_string()
        } else {
            format!("{dir}/{name}")
        }
    };
    if name == ".wh..wh..opq" {
        return Some((dir.to_string(), Kind::Opaque));
    }
    if let Some(target) = name.strip_prefix(".wh.") {
        return Some((join(target), Kind::Whiteout));
    }
    let kind = if tar_kind.is_dir() {
        Kind::Dir
    } else if tar_kind.is_symlink() {
        Kind::Symlink
    } else if tar_kind.is_hard_link() {
        Kind::Hardlink
    } else if tar_kind.is_file() || tar_kind == tar::EntryType::Continuous {
        Kind::File
    } else {
        Kind::Other
    };
    Some((path.to_string(), kind))
}

/// Walks one layer's tar stream, from the start, into its listing and the
/// checkpoints taken along the way. Blocking: run it off the async threads.
pub fn index_stream<R: Read>(input: R) -> io::Result<(Listing, Vec<Checkpoint>)> {
    let mut reader = BufReader::new(input);
    let compressed = reader.fill_buf()?.starts_with(&GZIP_MAGIC);
    let mut source = if compressed {
        Source::Gzip(Box::new(Inflater::new(reader, CHECKPOINT_EVERY)?))
    } else {
        Source::Plain(reader, 0)
    };

    let mut entries = Vec::new();
    // Scoped: the archive borrows the source, which is read again below.
    {
        let mut archive = tar::Archive::new(&mut source);
        for entry in archive.entries()? {
            let mut entry = entry?;
            let capabilities = entry_capabilities(&mut entry)?;
            let header = entry.header();
            let raw = entry.path()?.to_string_lossy().into_owned();
            let Some((path, kind)) = classify(&raw, header.entry_type()) else {
                continue;
            };
            let link = match kind {
                Kind::Symlink | Kind::Hardlink => entry
                    .link_name()?
                    .map(|link| link.to_string_lossy().into_owned()),
                _ => None,
            };
            let mut item = Entry {
                path,
                kind,
                size: header.size()?,
                mode: header.mode()?,
                uid: header.uid()?,
                gid: header.gid()?,
                mtime: header.mtime()?,
                link,
                offset: entry.raw_file_position(),
                content: None,
                capabilities,
            };
            if kind == Kind::File {
                item.content = Some(inspect(&item.path, &mut entry)?);
            }
            entries.push(item);
        }
    }

    let (uncompressed_size, checkpoints) = match source {
        Source::Gzip(inflater) => (inflater.position(), inflater.into_checkpoints()),
        Source::Plain(_, count) => (count, Vec::new()),
    };
    Ok((
        Listing {
            version: LISTING_VERSION,
            compressed,
            uncompressed_size,
            entries,
        },
        checkpoints,
    ))
}

/// Feeds `size` bytes of a gzipped layer's uncompressed stream, starting at
/// `offset`, to `sink` chunk by chunk. `input` is positioned at the
/// checkpoint's `in_offset`, or at the stream start without one. Blocking.
pub fn extract_gzip<R: Read>(
    input: R,
    checkpoint: Option<&Checkpoint>,
    offset: u64,
    size: u64,
    mut sink: impl FnMut(&[u8]) -> io::Result<()>,
) -> io::Result<()> {
    let mut inflater = match checkpoint {
        Some(checkpoint) => Inflater::resume(BufReader::new(input), checkpoint)?,
        None => Inflater::new(BufReader::new(input), u64::MAX)?,
    };
    inflater.skip(offset - inflater.position())?;
    let mut remaining = size;
    let mut chunk = vec![0u8; 64 * 1024];
    while remaining > 0 {
        let want = usize::try_from(remaining).map_or(chunk.len(), |n| n.min(chunk.len()));
        let read = inflater.read(&mut chunk[..want])?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "layer ends before the file does",
            ));
        }
        sink(&chunk[..read])?;
        remaining -= read as u64;
    }
    Ok(())
}

/// Reads a layer's stored listing; `None` when it was never indexed.
pub async fn read_listing(
    metadata_store: &MetadataStore,
    digest: &Digest,
) -> Result<Option<Listing>, RegistryError> {
    match metadata_store
        .object_store()
        .get(&digest.layer_entries_path())
        .await
    {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(angos_storage::Error::NotFound) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub async fn read_checkpoints(
    metadata_store: &MetadataStore,
    digest: &Digest,
) -> Result<Checkpoints, RegistryError> {
    match metadata_store
        .object_store()
        .get(&digest.layer_checkpoints_path())
        .await
    {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
        Err(angos_storage::Error::NotFound) => Ok(Checkpoints::default()),
        Err(e) => Err(e.into()),
    }
}

pub struct IndexLayerJobHandler {
    blob_store: Arc<BlobStore>,
    metadata_store: Arc<MetadataStore>,
}

fn job_error(error: &RegistryError) -> Error {
    Error::Execution(error.to_string())
}

impl IndexLayerJobHandler {
    pub fn new(blob_store: Arc<BlobStore>, metadata_store: Arc<MetadataStore>) -> Self {
        Self {
            blob_store,
            metadata_store,
        }
    }

    /// Indexes the layer unless a current listing already exists, or again
    /// when `force`; a layer whose bytes are gone has nothing to index and the
    /// job is done.
    pub async fn index(&self, digest: &Digest, force: bool) -> Result<(), Error> {
        let store = self.metadata_store.object_store();
        if !force
            && read_listing(&self.metadata_store, digest)
                .await
                .map_err(|e| job_error(&e))?
                .is_some_and(|listing| !listing.is_outdated())
        {
            return Ok(());
        }
        let (reader, _) = match self.blob_store.reader(digest, None).await {
            Ok(reader) => reader,
            Err(RegistryError::BlobUnknown) => {
                debug!("Index of {digest} skipped: the layer is gone");
                return Ok(());
            }
            Err(e) => return Err(job_error(&e)),
        };
        let (listing, checkpoints) =
            tokio::task::spawn_blocking(move || index_stream(SyncIoBridge::new(reader)))
                .await
                .map_err(|e| Error::Execution(format!("index task failed: {e}")))?
                .map_err(|e| {
                    Error::Execution(format!("layer {digest} is not a tar stream: {e}"))
                })?;
        let entries = listing.entries.len();
        let checkpoints = Checkpoints::from_inflater(checkpoints);
        store
            .put(
                &digest.layer_checkpoints_path(),
                Bytes::from(
                    serde_json::to_vec(&checkpoints)
                        .map_err(|e| Error::Execution(e.to_string()))?,
                ),
            )
            .await
            .map_err(|e| job_error(&e.into()))?;
        // The listing lands last: its presence is what marks the layer indexed.
        store
            .put(
                &digest.layer_entries_path(),
                Bytes::from(
                    serde_json::to_vec(&listing).map_err(|e| Error::Execution(e.to_string()))?,
                ),
            )
            .await
            .map_err(|e| job_error(&e.into()))?;
        info!(
            "Indexed layer {digest}: {entries} entries, {} checkpoints",
            checkpoints.items.len()
        );
        Ok(())
    }
}

#[async_trait]
impl JobHandler for IndexLayerJobHandler {
    async fn execute(&self, envelope: &JobEnvelope) -> Result<(), Error> {
        if envelope.kind != INDEX_LAYER_KIND {
            return Err(Error::Execution(format!(
                "unsupported job kind '{}'; expected '{INDEX_LAYER_KIND}'",
                envelope.kind,
            )));
        }
        let payload: IndexLayerPayload = serde_json::from_value(envelope.payload.clone())
            .map_err(|e| Error::Execution(format!("failed to deserialize job payload: {e}")))?;
        self.index(&payload.digest, payload.force).await
    }
}

#[cfg(test)]
mod tests;
