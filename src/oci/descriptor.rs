use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::oci::Digest;

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct Platform {
    pub architecture: String,
    pub os: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
    #[serde(default)]
    #[serde(rename = "os.version")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os_version: Option<String>,
    #[serde(default)]
    #[serde(rename = "os.features")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os_features: Option<Vec<String>>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub features: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Descriptor {
    pub media_type: String,
    pub digest: Digest,
    pub size: u64,
    #[serde(default)]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub annotations: HashMap<String, String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_type: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform: Option<Platform>,
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_descriptor_round_trip_all_fields() {
        let digest = Digest::from_str(
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .unwrap();
        let descriptor = Descriptor {
            media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
            digest,
            size: 1234,
            annotations: HashMap::from([
                (
                    "org.opencontainers.image.revision".to_string(),
                    "abc123".to_string(),
                ),
                ("custom.key".to_string(), "custom.value".to_string()),
            ]),
            artifact_type: Some("application/vnd.example.config".to_string()),
            platform: Some(Platform {
                architecture: "amd64".to_string(),
                os: "linux".to_string(),
                variant: Some("v1".to_string()),
                os_version: Some("22.04".to_string()),
                os_features: Some(vec!["feature1".to_string()]),
                features: Some(vec!["sse4".to_string()]),
            }),
        };

        let json = serde_json::to_string(&descriptor).unwrap();
        let round_tripped: Descriptor = serde_json::from_str(&json).unwrap();
        assert_eq!(descriptor, round_tripped);
    }

    #[test]
    fn test_descriptor_round_trip_minimal() {
        let digest = Digest::from_str(
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .unwrap();
        let descriptor = Descriptor {
            media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
            digest,
            size: 512,
            annotations: HashMap::new(),
            artifact_type: None,
            platform: None,
        };

        let json = serde_json::to_string(&descriptor).unwrap();

        // Optional/default fields must be omitted from serialized output
        assert!(!json.contains("annotations"));
        assert!(!json.contains("artifactType"));
        assert!(!json.contains("platform"));

        let round_tripped: Descriptor = serde_json::from_str(&json).unwrap();
        assert_eq!(descriptor, round_tripped);
    }

    #[test]
    fn test_descriptor_deserialize_ignores_unknown_fields() {
        let json = r#"{
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "size": 7682,
            "unknownField": "foo",
            "urls": ["https://example.com/blob"],
            "extra": {"nested": true}
        }"#;

        let descriptor: Descriptor = serde_json::from_str(json).unwrap();
        assert_eq!(
            descriptor.media_type,
            "application/vnd.oci.image.manifest.v1+json"
        );
        assert_eq!(descriptor.size, 7682);
        assert!(descriptor.annotations.is_empty());
        assert!(descriptor.artifact_type.is_none());
        assert!(descriptor.platform.is_none());
    }

    #[test]
    fn test_platform_serialization() {
        let platform = Platform {
            architecture: "amd64".to_string(),
            os: "linux".to_string(),
            variant: Some("v8".to_string()),
            os_version: Some("10.0.19041".to_string()),
            os_features: Some(vec!["win32k".to_string()]),
            features: Some(vec!["sse4".to_string()]),
        };

        let json = serde_json::to_string(&platform).unwrap();

        assert!(json.contains(r#""os.version":"10.0.19041""#));
        assert!(json.contains(r#""os.features":["win32k"]"#));
        assert!(json.contains(r#""architecture":"amd64""#));
        assert!(json.contains(r#""os":"linux""#));
    }

    #[test]
    fn test_platform_deserialization() {
        let json = r#"{
            "architecture": "arm64",
            "os": "linux",
            "variant": "v8",
            "os.version": "22.04",
            "os.features": ["feature1", "feature2"]
        }"#;

        let platform: Platform = serde_json::from_str(json).unwrap();

        assert_eq!(platform.architecture, "arm64");
        assert_eq!(platform.os, "linux");
        assert_eq!(platform.variant, Some("v8".to_string()));
        assert_eq!(platform.os_version, Some("22.04".to_string()));
        assert_eq!(
            platform.os_features,
            Some(vec!["feature1".to_string(), "feature2".to_string()])
        );
    }
}
