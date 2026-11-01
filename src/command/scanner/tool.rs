use std::process::Output;
use std::str::FromStr;

use tokio::process::Command;

use angos_mtls_client::BasicAuth;

use super::Error;

/// A supported vulnerability scanner. Each variant knows how to produce a SARIF
/// report on stdout for an image reference and how to pass registry
/// credentials to its own pull.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scanner {
    Grype,
    Trivy,
}

impl Scanner {
    /// Whether the scanner can only run one process at a time: Trivy locks
    /// its cache directory and a second process fails with "cache may be in
    /// use by another process", while Grype reads its database concurrently.
    #[must_use]
    pub fn serial(self) -> bool {
        matches!(self, Scanner::Trivy)
    }

    /// Whether the service keeps the scanner's vulnerability database fresh:
    /// Grype is told not to update during a scan, so the service pulls the
    /// database at start and once a day; Trivy refreshes its own.
    #[must_use]
    pub fn owns_database(self) -> bool {
        matches!(self, Scanner::Grype)
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Scanner::Grype => "grype",
            Scanner::Trivy => "trivy",
        }
    }

    /// Scans `image_ref` (a `host/namespace@digest` string) and returns the
    /// SARIF report bytes. `credentials` is the registry basic-auth pair the
    /// scanner pulls with; `plain_http` reaches a registry served over HTTP.
    pub async fn scan(
        self,
        image_ref: &str,
        plain_http: bool,
        credentials: Option<&BasicAuth>,
    ) -> Result<Vec<u8>, Error> {
        let mut command = Command::new(self.as_str());
        self.arguments(&mut command, image_ref);
        self.environment(&mut command, image_ref, plain_http, credentials);

        let output = self.run(command).await?;
        if output.stdout.is_empty() {
            return Err(Error::Scanner {
                scanner: self.as_str(),
                message: "produced no SARIF output".to_string(),
            });
        }
        Ok(output.stdout)
    }

    /// Pulls the vulnerability database of a scanner that [`owns_database`].
    ///
    /// [`owns_database`]: Scanner::owns_database
    pub async fn update_database(self) -> Result<(), Error> {
        if self.owns_database() {
            let mut command = Command::new(self.as_str());
            command.args(["db", "update"]);
            self.run(command).await?;
        }
        Ok(())
    }

    async fn run(self, mut command: Command) -> Result<Output, Error> {
        let output = command.output().await.map_err(|e| Error::Scanner {
            scanner: self.as_str(),
            message: format!("failed to run: {e}"),
        })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Scanner {
                scanner: self.as_str(),
                message: format!("exited with {}: {}", output.status, stderr.trim()),
            });
        }
        Ok(output)
    }

    fn arguments(self, command: &mut Command, image_ref: &str) {
        match self {
            Scanner::Grype => {
                command.args(["-o", "sarif", &format!("registry:{image_ref}")]);
            }
            Scanner::Trivy => {
                command.args([
                    "image",
                    "--quiet",
                    "--image-src",
                    "remote",
                    "--format",
                    "sarif",
                    image_ref,
                ]);
            }
        }
    }

    fn environment(
        self,
        command: &mut Command,
        image_ref: &str,
        plain_http: bool,
        credentials: Option<&BasicAuth>,
    ) {
        let authority = image_ref.split('/').next().unwrap_or(image_ref);
        match self {
            Scanner::Grype => {
                command.env("GRYPE_DB_AUTO_UPDATE", "false");
                if plain_http {
                    command.env("GRYPE_REGISTRY_INSECURE_USE_HTTP", "true");
                }
                if let Some(auth) = credentials {
                    command.env("SYFT_REGISTRY_AUTH_AUTHORITY", authority);
                    command.env("SYFT_REGISTRY_AUTH_USERNAME", &auth.username);
                    command.env("SYFT_REGISTRY_AUTH_PASSWORD", auth.password.expose());
                }
            }
            Scanner::Trivy => {
                if plain_http {
                    command.env("TRIVY_INSECURE", "true");
                }
                if let Some(auth) = credentials {
                    command.env("TRIVY_USERNAME", &auth.username);
                    command.env("TRIVY_PASSWORD", auth.password.expose());
                }
            }
        }
    }
}

impl FromStr for Scanner {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "grype" => Ok(Scanner::Grype),
            "trivy" => Ok(Scanner::Trivy),
            other => Err(format!(
                "unknown scanner '{other}'; expected 'grype' or 'trivy'"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;

    use tokio::process::Command;

    use angos_mtls_client::BasicAuth;
    use angos_secret::Secret;

    use super::Scanner;

    fn argv(command: &Command) -> Vec<String> {
        command
            .as_std()
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect()
    }

    fn env(command: &Command, key: &str) -> Option<String> {
        command
            .as_std()
            .get_envs()
            .find(|(k, _)| *k == OsStr::new(key))
            .and_then(|(_, v)| v.map(|v| v.to_string_lossy().into_owned()))
    }

    #[test]
    fn scanner_names_parse_and_nothing_else_does() {
        assert_eq!("grype".parse::<Scanner>(), Ok(Scanner::Grype));
        assert_eq!("trivy".parse::<Scanner>(), Ok(Scanner::Trivy));
        assert!("clair".parse::<Scanner>().is_err());
    }

    /// Grype pulls through syft, which takes the registry authority alone; a
    /// plain-HTTP registry needs its own switch.
    #[test]
    fn grype_gets_sarif_output_and_syft_registry_credentials() {
        let scanner = Scanner::Grype;
        let mut command = Command::new(scanner.as_str());
        let image = "registry.example.com:8000/apps/web@sha256:abc";
        scanner.arguments(&mut command, image);
        scanner.environment(
            &mut command,
            image,
            true,
            Some(&BasicAuth {
                username: "scanner".to_string(),
                password: Secret::new("pw".to_string()),
            }),
        );
        assert_eq!(
            argv(&command),
            ["-o", "sarif", &format!("registry:{image}")]
        );
        assert_eq!(
            env(&command, "GRYPE_DB_AUTO_UPDATE").as_deref(),
            Some("false")
        );
        assert_eq!(
            env(&command, "GRYPE_REGISTRY_INSECURE_USE_HTTP").as_deref(),
            Some("true")
        );
        assert_eq!(
            env(&command, "SYFT_REGISTRY_AUTH_AUTHORITY").as_deref(),
            Some("registry.example.com:8000")
        );
        assert_eq!(
            env(&command, "SYFT_REGISTRY_AUTH_USERNAME").as_deref(),
            Some("scanner")
        );
        assert_eq!(
            env(&command, "SYFT_REGISTRY_AUTH_PASSWORD").as_deref(),
            Some("pw")
        );
    }

    #[test]
    fn trivy_scans_the_remote_image_without_leaking_absent_credentials() {
        let scanner = Scanner::Trivy;
        let mut command = Command::new(scanner.as_str());
        let image = "registry.example.com/apps/web@sha256:abc";
        scanner.arguments(&mut command, image);
        scanner.environment(&mut command, image, false, None);
        assert_eq!(
            argv(&command),
            [
                "image",
                "--quiet",
                "--image-src",
                "remote",
                "--format",
                "sarif",
                image
            ]
        );
        assert!(env(&command, "TRIVY_USERNAME").is_none());
        assert!(env(&command, "TRIVY_INSECURE").is_none());
    }
}
