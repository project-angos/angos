//! Current-shape storage keys: the store roots, and every other key hung off
//! the type that owns it.
//!
//! `Digest` and `Namespace` belong to `angos-oci` and cannot take inherent
//! impls here, so their keys arrive as extension traits.

use std::str::FromStr;

use angos_oci::{Algorithm, Digest, Namespace, Tag, UploadSessionId};

use crate::registry::metadata_store::LinkKind;

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
        format!("{}/{namespace}!{}", self.blob_ref_dir(), link.ref_entry())
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
        match entry {
            "rev" => Some(LinkKind::Digest(self.clone())),
            "layer" => Some(LinkKind::Layer(self.clone())),
            "config" => Some(LinkKind::Config(self.clone())),
            _ => {
                if let Some(tag) = entry.strip_prefix("tag.") {
                    Some(LinkKind::Tag(Tag::new(tag).ok()?))
                } else if let Some(subject) = entry.strip_prefix("sub.") {
                    Some(LinkKind::Referrer {
                        subject: parse_ref_digest(subject)?,
                        referrer: self.clone(),
                    })
                } else if let Some(index) = entry.strip_prefix("idx.") {
                    Some(LinkKind::Manifest {
                        index: parse_ref_digest(index)?,
                        child: self.clone(),
                    })
                } else {
                    // A bare `<algo>.<hash>` names the referring manifest,
                    // unambiguous against the prefixed shapes because no
                    // algorithm is named `rev`, `layer`, `config`, `tag`,
                    // `sub`, or `idx`.
                    Some(LinkKind::ReferencedBy(parse_ref_digest(entry)?))
                }
            }
        }
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
    fn tag_entry_path(&self, tag: &Tag, ord: u64, deletion: bool, digest: &Digest) -> String;

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

    fn tag_entry_path(&self, tag: &Tag, ord: u64, deletion: bool, digest: &Digest) -> String {
        let kind = if deletion { "del" } else { "set" };
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

    fn referrer_record_dir(&self, subject: &Digest) -> String {
        format!(
            "{NS_ROOT}/{self}!sub/{}/{}/{}",
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
    use chrono::DateTime;

    use crate::registry::{keys::*, metadata_store::tag_ord};

    const HASH_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HASH_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const HASH_512: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

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
        let entry_key = ns.tag_entry_path(&tag, 7, true, &digest);
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

        let older_key = ns.tag_entry_path(&tag, tag_ord(Some(older)), false, &digest);
        let newer_key = ns.tag_entry_path(&tag, tag_ord(Some(newer)), true, &digest);
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
    }

    #[test]
    fn blob_ref_paths_round_trip() {
        let ns = Namespace::new("org/app").unwrap();
        let digest = Digest::sha256(HASH_A).unwrap();
        let other = Digest::sha256(HASH_B).unwrap();
        let links = [
            LinkKind::Blob(digest.clone()),
            LinkKind::Digest(digest.clone()),
            LinkKind::Layer(digest.clone()),
            LinkKind::Config(digest.clone()),
            LinkKind::Tag(Tag::new("v1.2-rc.1_x").unwrap()),
            LinkKind::Referrer {
                subject: other.clone(),
                referrer: digest.clone(),
            },
            LinkKind::Manifest {
                index: other.clone(),
                child: digest.clone(),
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
        let layer_key = digest.blob_ref_path(&ns, &LinkKind::Layer(digest.clone()));
        assert_eq!(
            layer_key,
            format!("{}/layer", digest.blob_ref_namespace_dir(&ns))
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
