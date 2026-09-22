//! Current-shape storage keys: the store roots, and every other key hung off
//! the type that owns it.
//!
//! `Digest` and `Namespace` belong to `angos-oci` and cannot take inherent
//! impls here, so their keys arrive as extension traits.

use std::str::FromStr;

use chrono::{DateTime, Utc};

use angos_oci::{Algorithm, Digest, Namespace, Tag, UploadSessionId};

use crate::registry::metadata_store::LinkKind;

/// The reference-key tail after `<ns>!`. Digest-bearing kinds omit the blob's
/// own digest and spell out only the foreign one: a referrer entry names its
/// subject, a per-referrer entry the manifest that references the blob.
/// [`DigestKeys::parse_blob_ref_entry`] inverts it.
fn ref_tail(link: &LinkKind) -> String {
    match link {
        LinkKind::Blob(_) => "own".to_string(),
        LinkKind::Digest(_) => "r/rev".to_string(),
        LinkKind::Tag(tag) => format!("r/tag.{tag}"),
        LinkKind::Referrer { subject, .. } => {
            format!("r/sub.{}.{}", subject.algorithm(), subject.hash())
        }
        LinkKind::ReferencedBy(referrer) => {
            format!("r/{}.{}", referrer.algorithm(), referrer.hash())
        }
    }
}

/// One tag entry, decoded from its key name: what the entry records, plus the
/// ordinal that orders and groups entries straight off a listing and the stamp
/// that ordinal encodes.
#[derive(Debug, Eq, PartialEq)]
pub enum TagEntry {
    /// A push: the tag points at `digest`.
    Set {
        ord: u64,
        authored_at: Option<DateTime<Utc>>,
        digest: Digest,
    },
    /// A delete: the tag holds nothing, and `held` names the digest it pointed
    /// at, which tag history requires a tombstone to carry.
    Deletion {
        ord: u64,
        authored_at: Option<DateTime<Utc>>,
        held: Digest,
    },
}

impl TagEntry {
    /// The inverted-millis ordinal, which orders entries newest-first and
    /// groups the ones authored in the same millisecond.
    #[must_use]
    pub fn ord(&self) -> u64 {
        match self {
            TagEntry::Set { ord, .. } | TagEntry::Deletion { ord, .. } => *ord,
        }
    }

    /// The stamp the ordinal encodes; `None` for an ordinal outside the range
    /// a writer produces.
    #[must_use]
    pub fn authored_at(&self) -> Option<DateTime<Utc>> {
        match self {
            TagEntry::Set { authored_at, .. } | TagEntry::Deletion { authored_at, .. } => {
                *authored_at
            }
        }
    }

    /// The digest the entry names: the tag's target, or what a delete ended.
    #[must_use]
    pub fn digest(&self) -> &Digest {
        match self {
            TagEntry::Set { digest, .. } => digest,
            TagEntry::Deletion { held, .. } => held,
        }
    }
}

impl FromStr for TagEntry {
    /// Nothing to report: a caller skips or quarantines the key, and a
    /// listing parses every name it walks, so the rejection must not
    /// allocate.
    type Err = ();

    /// Decode one entry file of [`NamespaceKeys::tag_entry_dir`].
    fn from_str(name: &str) -> Result<Self, Self::Err> {
        let mut parts = name.splitn(4, '.');
        let (Some(ord), Some(kind), Some(algorithm), Some(hash)) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(());
        };
        if ord.len() != 16 {
            return Err(());
        }
        let ord = u64::from_str_radix(ord, 16).map_err(|_| ())?;
        // No stamp for an ordinal outside the band a writer produces, such as
        // the `u64::MAX` an angos before 1.9.0 wrote for an entry converted
        // from a stamp-less link: those sort last and never win resolution.
        let authored_at = (u64::MAX - 1)
            .checked_sub(ord)
            .and_then(|millis| i64::try_from(millis).ok())
            .and_then(DateTime::from_timestamp_millis);
        let algorithm = Algorithm::from_str(algorithm).map_err(|_| ())?;
        let digest = Digest::with_algorithm(algorithm, hash).map_err(|_| ())?;
        match kind {
            "set" => Ok(TagEntry::Set {
                ord,
                authored_at,
                digest,
            }),
            "del" => Ok(TagEntry::Deletion {
                ord,
                authored_at,
                held: digest,
            }),
            _ => Err(()),
        }
    }
}

/// The stamp time one entry file of an atime directory records. `None` = not a
/// shape this version writes.
pub fn parse_atime_entry(name: &str) -> Option<DateTime<Utc>> {
    let (ord, suffix) = name.split_once('.')?;
    if ord.len() != 16 || suffix.len() != 8 || !suffix.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    (u64::MAX - 1)
        .checked_sub(u64::from_str_radix(ord, 16).ok()?)
        .and_then(|millis| i64::try_from(millis).ok())
        .and_then(DateTime::from_timestamp_millis)
}

/// What follows an atime entry directory's `!` terminator on its compacted
/// sibling.
pub const ATIME_COMPACTED: &str = "compacted";

/// The store roots. They live together because the maintenance walk matches
/// all six in one dispatch.
pub const BLOBS_ROOT: &str = "v2/blobs";
pub const REPOS_ROOT: &str = "v2/repositories";
pub const REF_ROOT: &str = "v2/ref";
pub const NS_ROOT: &str = "v2/ns";
pub const CAT_ROOT: &str = "v2/cat";
pub const GC_ROOT: &str = "v2/gc";
/// Layer listings and inflater checkpoints, by layer digest, in the metadata store.
pub const LAYERS_ROOT: &str = "v2/layers";

/// Every current-shape storage key addressed by a blob's digest.
pub trait DigestKeys {
    /// Directory holding a blob's data.
    fn blob_dir(&self) -> String;

    /// The blob's content.
    fn blob_path(&self) -> String;

    /// Directory holding what the filesystem indexer derived from the layer.
    fn layer_dir(&self) -> String;

    /// The layer's entry listing, whose presence marks the layer indexed.
    fn layer_entries_path(&self) -> String;

    /// The inflater's checkpoints into the layer.
    fn layer_checkpoints_path(&self) -> String;

    /// Directory holding every reference key for the digest, one key per
    /// (namespace, link), rooted outside the blob store's `v2/blobs/` tree.
    fn blob_ref_dir(&self) -> String;

    /// One namespace's reference key for the digest: ownership is the
    /// `<ns>!own` leaf, every other link kind a leaf under `<ns>!r/`. `!`
    /// terminates the namespace because it is outside the namespace grammar,
    /// so the name always parses back out and `a`'s leaves never collide with
    /// `a/b`'s directories.
    fn blob_ref_path(&self, namespace: &Namespace, link: &LinkKind) -> String;

    /// The namespace's ownership reference key for the digest.
    fn blob_ref_own_path(&self, namespace: &Namespace) -> String;

    /// Directory holding a namespace's non-ownership reference keys for the
    /// digest, a directory boundary on both backends so it lists without
    /// partial-name prefix support.
    fn blob_ref_namespace_dir(&self, namespace: &Namespace) -> String;

    /// Decode one key of [`DigestKeys::blob_ref_dir`], relative to that
    /// directory, into its raw namespace (validity is the caller's concern)
    /// and the link it records.
    fn parse_blob_ref(&self, key: &str) -> Option<(String, LinkKind)>;

    /// Decode one key of [`DigestKeys::blob_ref_namespace_dir`], relative to
    /// that directory.
    fn parse_blob_ref_entry(&self, entry: &str) -> Option<LinkKind>;
}

impl DigestKeys for Digest {
    fn blob_dir(&self) -> String {
        format!(
            "{BLOBS_ROOT}/{}/{}/{}",
            self.algorithm(),
            self.hash_prefix(),
            self.hash()
        )
    }

    fn blob_path(&self) -> String {
        format!("{}/data", self.blob_dir())
    }

    fn layer_dir(&self) -> String {
        format!(
            "{LAYERS_ROOT}/{}/{}/{}",
            self.algorithm(),
            self.hash_prefix(),
            self.hash()
        )
    }

    fn layer_entries_path(&self) -> String {
        format!("{}/entries", self.layer_dir())
    }

    fn layer_checkpoints_path(&self) -> String {
        format!("{}/checkpoints", self.layer_dir())
    }

    fn blob_ref_dir(&self) -> String {
        format!(
            "{REF_ROOT}/{}/{}/{}",
            self.algorithm(),
            self.hash_prefix(),
            self.hash()
        )
    }

    fn blob_ref_path(&self, namespace: &Namespace, link: &LinkKind) -> String {
        format!("{}/{namespace}!{}", self.blob_ref_dir(), ref_tail(link))
    }

    fn blob_ref_own_path(&self, namespace: &Namespace) -> String {
        format!("{}/{namespace}!own", self.blob_ref_dir())
    }

    fn blob_ref_namespace_dir(&self, namespace: &Namespace) -> String {
        format!("{}/{namespace}!r", self.blob_ref_dir())
    }

    fn parse_blob_ref(&self, key: &str) -> Option<(String, LinkKind)> {
        let (namespace, entry) = key.split_once('!')?;
        let link = match entry.strip_prefix("r/") {
            None => (entry == "own").then(|| LinkKind::Blob(self.clone()))?,
            Some(entry) => self.parse_blob_ref_entry(entry)?,
        };
        Some((namespace.to_string(), link))
    }

    fn parse_blob_ref_entry(&self, entry: &str) -> Option<LinkKind> {
        if entry == "rev" {
            return Some(LinkKind::Digest(self.clone()));
        }
        if let Some(tag) = entry.strip_prefix("tag.") {
            return Some(LinkKind::Tag(Tag::new(tag).ok()?));
        }
        if let Some(subject) = entry.strip_prefix("sub.") {
            return Some(LinkKind::Referrer {
                subject: parse_ref_digest(subject)?,
                referrer: self.clone(),
            });
        }
        // A bare `<algo>.<hash>` names the referring manifest, unambiguous
        // against the prefixed shapes because no algorithm is named `rev`,
        // `tag` or `sub`.
        Some(LinkKind::ReferencedBy(parse_ref_digest(entry)?))
    }
}

/// `<algo>.<hash>` inside a reference-key entry; `.` separates unambiguously
/// because algorithm names never contain it.
fn parse_ref_digest(s: &str) -> Option<Digest> {
    let (algorithm, hash) = s.split_once('.')?;
    Digest::with_algorithm(Algorithm::from_str(algorithm).ok()?, hash).ok()
}

/// The layer a listing key names, `v2/layers/<algorithm>/<prefix>/<hash>/...`.
pub fn parse_layer_key(key: &str) -> Option<Digest> {
    let mut parts = key.strip_prefix(LAYERS_ROOT)?.strip_prefix('/')?.split('/');
    let algorithm = Algorithm::from_str(parts.next()?).ok()?;
    let _prefix = parts.next()?;
    Digest::with_algorithm(algorithm, parts.next()?).ok()
}

/// Every current-shape storage key addressed by a namespace.
pub trait NamespaceKeys {
    /// Directory holding every tag-entry directory of the namespace,
    /// `!`-terminated for the same reasons as the reference keys.
    fn tag_entries_root(&self) -> String;

    /// Directory holding one tag's ordered entries. The `!` suffix keeps a
    /// name that is a prefix of another (`v1`, `v1.1`) sorting first in a flat
    /// listing, which is what lets the tag list serve lexical order straight
    /// off it.
    fn tag_entry_dir(&self, tag: &Tag) -> String;

    /// One tag event: `<ord>.<kind>.<algo>.<hash>`, where `<ord>` inverts the
    /// author's unix-millisecond timestamp so entries list newest first, and
    /// `<kind>` is `set` or `del` (a deletion still names the digest the tag
    /// held, which tag history requires).
    fn tag_entry_path(
        &self,
        tag: &Tag,
        authored_at: DateTime<Utc>,
        deletion: bool,
        digest: &Digest,
    ) -> String;

    /// One tag's demoted entry, under a `!`-terminated history directory. It
    /// keeps its [`NamespaceKeys::tag_entry_path`] file name, so history stays
    /// in newest-first order.
    fn tag_hist_path(&self, tag: &Tag, entry_name: &str) -> String;

    /// Directory holding one tag's append-only access entries, `!`-terminated
    /// like [`NamespaceKeys::tag_entry_dir`] so a tag named like another's
    /// prefix cannot collide with it.
    fn tag_atime_entry_dir(&self, tag: &Tag) -> String;

    /// Directory holding one revision's append-only access entries.
    fn revision_atime_entry_dir(&self, digest: &Digest) -> String;

    /// The directory holding `link`'s access entries. Only tags and revisions
    /// are pull-tracked, so every other kind has none.
    fn atime_dir(&self, link: &LinkKind) -> Option<String>;

    /// The access entry a pull of `link` at `at` by `client` records, named
    /// `<ord>.<suffix>`: `<ord>` is the same inverted-millis ordinal a tag
    /// entry carries, so entries list newest first, and `<suffix>` is the
    /// first 8 hex of the client identity's sha256, so two clients stamping in
    /// the same millisecond land on distinct entries instead of one
    /// overwriting the other's audit record. `None` for a kind that is not
    /// pull-tracked.
    fn atime_entry_path(&self, link: &LinkKind, at: DateTime<Utc>, client: &str) -> Option<String>;

    /// Directory holding `link`'s compacted access entries, a sibling of its
    /// entry directory. Each chunk is named after its newest entry, so chunks
    /// list newest first too.
    fn atime_compacted_dir(&self, link: &LinkKind) -> Option<String>;

    /// The namespace's catalog index key: empty, write-once, one per
    /// namespace. The `!` terminator is what lets `a` and `a/b` coexist on FS
    /// (a file cannot also be a directory) while keeping the flat listing in
    /// lexical order.
    fn catalog_index_path(&self) -> String;

    /// The immutable record of a stored manifest revision. Its existence is
    /// what makes the digest resolvable; its body carries what a HEAD needs.
    fn revision_record_path(&self, digest: &Digest) -> String;

    /// Directory holding every revision record of the namespace.
    fn revision_records_root(&self) -> String;

    /// Directory holding `subject`'s referrer records: one key per referring
    /// manifest, whose body is that manifest's descriptor.
    /// Root of every referrer record in the namespace, which one walk lists
    /// where reading per subject would list once each.
    fn referrer_records_root(&self) -> String;
    fn referrer_record_dir(&self, subject: &Digest) -> String;

    /// One referring manifest's record under `subject`.
    fn referrer_record_path(&self, subject: &Digest, referrer: &Digest) -> String;

    /// Root directory holding the namespace's upload containers, one per
    /// session.
    fn uploads_root_dir(&self) -> String;

    /// Directory holding everything one upload session owns.
    fn upload_container_path(&self, session_id: &UploadSessionId) -> String;

    /// The bytes received so far for an upload session.
    fn upload_path(&self, session_id: &UploadSessionId) -> String;

    /// The upload session's single durable record: last activity, committed
    /// offset, and the serialised hasher checkpoint, rewritten on every
    /// activity.
    fn upload_session_path(&self, session_id: &UploadSessionId) -> String;
}

impl NamespaceKeys for Namespace {
    fn tag_entries_root(&self) -> String {
        format!("{NS_ROOT}/{self}!tag")
    }

    fn tag_entry_dir(&self, tag: &Tag) -> String {
        format!("{}/{tag}!", self.tag_entries_root())
    }

    fn tag_entry_path(
        &self,
        tag: &Tag,
        authored_at: DateTime<Utc>,
        deletion: bool,
        digest: &Digest,
    ) -> String {
        let kind = if deletion { "del" } else { "set" };
        // Inverted millis, so a listing yields the newest entry first.
        let ord = u64::MAX - 1 - authored_at.timestamp_millis().max(0).unsigned_abs();
        format!(
            "{}/{ord:016x}.{kind}.{}.{}",
            self.tag_entry_dir(tag),
            digest.algorithm(),
            digest.hash()
        )
    }

    fn tag_hist_path(&self, tag: &Tag, entry_name: &str) -> String {
        format!("{NS_ROOT}/{self}!hist/{tag}!/{entry_name}")
    }

    fn tag_atime_entry_dir(&self, tag: &Tag) -> String {
        format!("{NS_ROOT}/{self}!atime/tag/{tag}!")
    }

    fn atime_dir(&self, link: &LinkKind) -> Option<String> {
        match link {
            LinkKind::Tag(tag) => Some(self.tag_atime_entry_dir(tag)),
            LinkKind::Digest(digest) => Some(self.revision_atime_entry_dir(digest)),
            _ => None,
        }
    }

    fn atime_entry_path(&self, link: &LinkKind, at: DateTime<Utc>, client: &str) -> Option<String> {
        let identity = Digest::sha256_of_bytes(client.as_bytes());
        let ord = u64::MAX - 1 - at.timestamp_millis().max(0).unsigned_abs();
        Some(format!(
            "{}/{ord:016x}.{}",
            self.atime_dir(link)?,
            &identity.hash()[..8]
        ))
    }

    fn atime_compacted_dir(&self, link: &LinkKind) -> Option<String> {
        Some(format!("{}{ATIME_COMPACTED}", self.atime_dir(link)?))
    }

    fn revision_atime_entry_dir(&self, digest: &Digest) -> String {
        format!(
            "{NS_ROOT}/{self}!atime/rev/{}/{}!",
            digest.algorithm(),
            digest.hash()
        )
    }

    fn catalog_index_path(&self) -> String {
        format!("{CAT_ROOT}/{self}!")
    }

    fn revision_record_path(&self, digest: &Digest) -> String {
        format!(
            "{}/{}/{}/{}",
            self.revision_records_root(),
            digest.algorithm(),
            digest.hash_prefix(),
            digest.hash()
        )
    }

    fn revision_records_root(&self) -> String {
        format!("{NS_ROOT}/{self}!rev")
    }

    fn referrer_records_root(&self) -> String {
        format!("{NS_ROOT}/{self}!sub")
    }

    fn referrer_record_dir(&self, subject: &Digest) -> String {
        format!(
            "{}/{}/{}/{}",
            self.referrer_records_root(),
            subject.algorithm(),
            subject.hash_prefix(),
            subject.hash()
        )
    }

    fn referrer_record_path(&self, subject: &Digest, referrer: &Digest) -> String {
        format!(
            "{}/{}.{}",
            self.referrer_record_dir(subject),
            referrer.algorithm(),
            referrer.hash()
        )
    }

    fn uploads_root_dir(&self) -> String {
        format!("{REPOS_ROOT}/{self}/_uploads")
    }

    fn upload_container_path(&self, session_id: &UploadSessionId) -> String {
        format!("{}/{session_id}", self.uploads_root_dir())
    }

    fn upload_path(&self, session_id: &UploadSessionId) -> String {
        format!("{}/data", self.upload_container_path(session_id))
    }

    fn upload_session_path(&self, session_id: &UploadSessionId) -> String {
        format!("{}/session.json", self.upload_container_path(session_id))
    }
}

/// Storage prefix for a namespace subtree addressed by its raw on-disk name, so
/// scrub can reclaim a directory whose name fails `Namespace` validation. A
/// free function rather than a [`NamespaceKeys`] method precisely because no
/// `Namespace` exists for such a name. `None` when a segment is empty, `.`, or
/// `..`, which could escape the root.
pub fn namespace_dir(name: &str) -> Option<String> {
    if name.is_empty()
        || name
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return None;
    }
    Some(format!("{REPOS_ROOT}/{name}"))
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeDelta};

    use crate::registry::keys::*;

    const HASH_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HASH_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const HASH_512: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

    /// The name an entry key carries encodes its stamp at millisecond
    /// precision, and an ordinal outside the band a writer produces reads as
    /// no stamp while sorting after every real one.
    #[test]
    fn an_entry_name_encodes_its_stamp_and_a_foreign_ordinal_reads_as_none() {
        let namespace = Namespace::new("org/app").unwrap();
        let tag = Tag::new("v1.0").unwrap();
        let digest = Digest::sha256(HASH_A).unwrap();
        let at = DateTime::from_timestamp_millis(1_700_000_000_123).unwrap();

        let entry_name = |at| {
            namespace
                .tag_entry_path(&tag, at, false, &digest)
                .rsplit_once('/')
                .unwrap()
                .1
                .to_string()
        };
        let name = entry_name(at);
        assert_eq!(name.parse::<TagEntry>().unwrap().authored_at(), Some(at));

        // Sub-millisecond precision is not encoded, so a stamp truncates to
        // the same name.
        assert_eq!(entry_name(at + TimeDelta::microseconds(400)), name);

        let foreign = format!(
            "{:016x}.set.{}.{}",
            u64::MAX,
            digest.algorithm(),
            digest.hash()
        );
        assert_eq!(
            foreign.parse::<TagEntry>().unwrap().authored_at(),
            None,
            "the ordinal an angos before 1.9.0 wrote for a stamp-less entry"
        );
        assert!(
            foreign > entry_name(DateTime::from_timestamp_millis(0).unwrap()),
            "such an entry must sort after a real epoch one"
        );
    }

    #[test]
    fn entry_names_round_trip_through_the_parser() {
        let namespace = Namespace::new("org/app").unwrap();
        let tag = Tag::new("v1.0").unwrap();
        let digest = Digest::sha256(HASH_A).unwrap();
        let at = DateTime::from_timestamp_millis(1_700_000_000_123).unwrap();

        for deletion in [false, true] {
            let key = namespace.tag_entry_path(&tag, at, deletion, &digest);
            let (_, name) = key.rsplit_once('/').unwrap();
            let parsed = name.parse::<TagEntry>().expect("entry must parse");
            let expected = if deletion {
                TagEntry::Deletion {
                    ord: parsed.ord(),
                    authored_at: Some(at),
                    held: digest.clone(),
                }
            } else {
                TagEntry::Set {
                    ord: parsed.ord(),
                    authored_at: Some(at),
                    digest: digest.clone(),
                }
            };
            assert_eq!(
                name.parse::<TagEntry>(),
                Ok(expected),
                "entry {name:?} must round-trip"
            );
        }

        // Same millisecond, different digests: distinct keys, both parseable.
        let other = Digest::sha256(HASH_B).unwrap();
        assert_ne!(
            namespace.tag_entry_path(&tag, at, false, &digest),
            namespace.tag_entry_path(&tag, at, false, &other)
        );
    }

    #[test]
    fn foreign_entry_names_do_not_parse() {
        for name in [
            "",
            &format!("{:016x}.set.sha256", 1_u64),
            &format!("{:08x}.set.sha256.{HASH_A}", 1_u64),
            &format!("{:016x}.mov.sha256.{HASH_A}", 1_u64),
            &format!("{:016x}.set.sha3.{HASH_A}", 1_u64),
            &format!("{:016x}.set.sha256.{}", 1_u64, "z".repeat(64)),
            &format!("{:016x}.set.sha256.{HASH_A}.extra", 1_u64),
        ] {
            assert!(name.parse::<TagEntry>().is_err(), "name {name:?}");
        }
    }

    #[test]
    fn test_blob_paths() {
        let digest = Digest::sha256(HASH_A).unwrap();
        assert_eq!(
            digest.blob_path(),
            format!("v2/blobs/sha256/aa/{HASH_A}/data")
        );
        assert_eq!(digest.blob_dir(), format!("v2/blobs/sha256/aa/{HASH_A}"));
    }

    #[test]
    fn test_blob_paths_sha512() {
        let digest = Digest::sha512(HASH_512).unwrap();
        assert_eq!(
            digest.blob_path(),
            format!("v2/blobs/sha512/cc/{HASH_512}/data")
        );
    }

    #[test]
    fn test_upload_paths() {
        let ns = Namespace::new("ns").unwrap();
        let id = UploadSessionId::new("067e6162-3b6f-4ae2-a171-2470b63dff00").unwrap();
        assert_eq!(
            ns.upload_container_path(&id),
            format!("v2/repositories/ns/_uploads/{id}")
        );
        assert_eq!(
            ns.upload_path(&id),
            format!("v2/repositories/ns/_uploads/{id}/data")
        );
        assert_eq!(ns.uploads_root_dir(), "v2/repositories/ns/_uploads");
        assert_eq!(
            ns.upload_session_path(&id),
            format!("v2/repositories/ns/_uploads/{id}/session.json")
        );
    }

    #[test]
    fn test_namespace_dir() {
        assert_eq!(namespace_dir("ns").unwrap(), "v2/repositories/ns");
        assert_eq!(namespace_dir("org/app").unwrap(), "v2/repositories/org/app");
        // Uppercase fails `Namespace` validation but is safe as a directory.
        assert_eq!(namespace_dir("BadNS").unwrap(), "v2/repositories/BadNS");
        for unsafe_name in ["", "..", ".", "a/../b", "a//b", "/a", "a/", "a/."] {
            assert!(
                namespace_dir(unsafe_name).is_none(),
                "'{unsafe_name}' must be rejected"
            );
        }
    }

    /// Flat listings stay in lexical order only while `!` sorts below every
    /// byte the namespace and tag grammars admit; probed against the real
    /// validators so a grammar relaxation fails here.
    #[test]
    fn the_separator_sorts_below_both_grammars() {
        for byte in 0u8..=127 {
            let c = byte as char;
            let admitted =
                Namespace::new(&format!("a{c}a")).is_ok() || Tag::new(&format!("a{c}a")).is_ok();
            if admitted {
                assert!(
                    b'!' < byte,
                    "'!' must sort below {c:?} or listings lose lexical order"
                );
            }
        }
    }

    #[test]
    fn tag_hist_paths_mirror_tag_entry_paths() {
        let ns = Namespace::new("org/app").unwrap();
        let tag = Tag::new("v1").unwrap();
        let digest = Digest::sha256(HASH_A).unwrap();
        let entry_key = ns.tag_entry_path(
            &tag,
            DateTime::from_timestamp_millis(7).unwrap(),
            true,
            &digest,
        );
        let file = entry_key.rsplit_once('/').unwrap().1;
        let hist_key = ns.tag_hist_path(&tag, file);
        assert_eq!(hist_key, format!("v2/ns/org/app!hist/v1!/{file}"));
    }

    #[test]
    fn tag_entries_sort_newest_first() {
        let ns = Namespace::new("org/app").unwrap();
        let tag = Tag::new("v1").unwrap();
        let digest = Digest::sha256(HASH_A).unwrap();
        let older = DateTime::from_timestamp_millis(1_000_000).unwrap();
        let newer = DateTime::from_timestamp_millis(2_000_000).unwrap();

        let older_key = ns.tag_entry_path(&tag, older, false, &digest);
        let newer_key = ns.tag_entry_path(&tag, newer, true, &digest);
        assert!(
            newer_key < older_key,
            "a newer entry must sort before an older one"
        );
    }

    #[test]
    fn atime_entry_dirs_sit_under_the_namespace_tree() {
        let ns = Namespace::new("org/app").unwrap();
        let tag = Tag::new("v1").unwrap();
        let digest = Digest::sha256(HASH_A).unwrap();
        assert_eq!(ns.tag_atime_entry_dir(&tag), "v2/ns/org/app!atime/tag/v1!");
        assert_eq!(
            ns.revision_atime_entry_dir(&digest),
            format!("v2/ns/org/app!atime/rev/sha256/{HASH_A}!")
        );
        assert_eq!(
            ns.atime_compacted_dir(&LinkKind::Tag(tag)).unwrap(),
            "v2/ns/org/app!atime/tag/v1!compacted"
        );
    }

    #[test]
    fn blob_ref_paths_round_trip() {
        let ns = Namespace::new("org/app").unwrap();
        let digest = Digest::sha256(HASH_A).unwrap();
        let other = Digest::sha256(HASH_B).unwrap();
        let links = [
            LinkKind::Blob(digest.clone()),
            LinkKind::Digest(digest.clone()),
            LinkKind::Tag(Tag::new("v1.2-rc.1_x").unwrap()),
            LinkKind::Referrer {
                subject: other.clone(),
                referrer: digest.clone(),
            },
            LinkKind::ReferencedBy(other.clone()),
        ];
        let dir = digest.blob_ref_dir();
        for link in links {
            let key = digest.blob_ref_path(&ns, &link);
            let relative = key.strip_prefix(&format!("{dir}/")).unwrap();
            assert_eq!(
                digest.parse_blob_ref(relative),
                Some(("org/app".to_string(), link.clone())),
                "{link} must round-trip through its reference key"
            );
        }
    }

    #[test]
    fn blob_ref_own_and_namespace_dirs_agree_with_the_full_paths() {
        let ns = Namespace::new("org/app").unwrap();
        let digest = Digest::sha256(HASH_A).unwrap();
        assert_eq!(
            digest.blob_ref_own_path(&ns),
            digest.blob_ref_path(&ns, &LinkKind::Blob(digest.clone()))
        );
        assert_eq!(
            digest.blob_ref_path(&ns, &LinkKind::Digest(digest.clone())),
            format!("{}/rev", digest.blob_ref_namespace_dir(&ns))
        );
    }

    /// `org`'s leaf keys must not be mistaken for `org/app`'s, the whole point
    /// of terminating the namespace with a byte its grammar cannot hold.
    #[test]
    fn a_namespace_that_prefixes_another_keeps_its_own_keys() {
        let digest = Digest::sha256(HASH_A).unwrap();
        let parent = Namespace::new("org").unwrap();
        let child = Namespace::new("org/app").unwrap();
        let dir = format!("{}/", digest.blob_ref_dir());

        for (namespace, expected) in [(&parent, "org"), (&child, "org/app")] {
            let key = digest.blob_ref_own_path(namespace);
            let relative = key.strip_prefix(&dir).unwrap();
            assert_eq!(
                digest.parse_blob_ref(relative),
                Some((expected.to_string(), LinkKind::Blob(digest.clone())))
            );
        }
        assert_ne!(
            digest.blob_ref_own_path(&parent),
            digest.blob_ref_own_path(&child)
        );
    }

    #[test]
    fn foreign_ref_shapes_do_not_parse() {
        let digest = Digest::sha256(HASH_A).unwrap();
        for key in [
            "ns",
            "ns!x",
            "ns!r/unknown",
            "ns!r/tag.",
            "ns!r/sub.sha256",
            "ns!r/sub.sha3.abcd",
            &format!("ns!r/idx.sha256.{}", "z".repeat(64)),
            "ns!r/sha256",
            "ns!r/sha3.abcd",
            &format!("ns!r/sha256.{}", "z".repeat(64)),
        ] {
            assert_eq!(digest.parse_blob_ref(key), None, "key {key:?}");
        }
    }
}
