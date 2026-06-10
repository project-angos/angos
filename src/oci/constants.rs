pub const IN_TOTO_PREDICATE_TYPE: &str = "in-toto.io/predicate-type";
pub const DOCKER_REFERENCE_DIGEST: &str = "vnd.docker.reference.digest";

/// Media type of an OCI image manifest.
pub const OCI_MANIFEST_MEDIA_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";
/// Media type of an OCI image index (manifest list).
pub const OCI_INDEX_MEDIA_TYPE: &str = "application/vnd.oci.image.index.v1+json";
/// Media type of a Docker schema-2 image manifest.
pub const DOCKER_MANIFEST_MEDIA_TYPE: &str = "application/vnd.docker.distribution.manifest.v2+json";
/// Media type of a Docker schema-2 manifest list.
pub const DOCKER_MANIFEST_LIST_MEDIA_TYPE: &str =
    "application/vnd.docker.distribution.manifest.list.v2+json";
