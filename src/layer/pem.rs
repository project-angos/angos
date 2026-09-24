//! The blocks of a PEM file, its certificates decoded.

use chrono::DateTime;
use x509_parser::{extensions::GeneralName, pem::Pem};

use angos_extension_service::{Certificate, PemBlock};

/// What a PEM file's certificates are read out of at most, past any real bundle.
pub const PEM_LIMIT: u64 = 4 * 1024 * 1024;

/// Whether a file's first bytes hold a certificate, which is worth decoding.
pub fn holds_certificates(head: &[u8]) -> bool {
    const MARKER: &[u8] = b"-----BEGIN CERTIFICATE-----";
    head.windows(MARKER.len()).any(|window| window == MARKER)
}

/// Every block of `text` in order, its certificates decoded where they can be.
pub fn blocks(text: &[u8]) -> Vec<PemBlock> {
    Pem::iter_from_buffer(text)
        .map_while(Result::ok)
        .map(|pem| {
            let certificate = if pem.label == "CERTIFICATE" {
                certificate(&pem)
            } else {
                None
            };
            PemBlock {
                label: pem.label,
                certificate,
            }
        })
        .collect()
}

fn certificate(pem: &Pem) -> Option<Certificate> {
    let x509 = pem.parse_x509().ok()?;
    let validity = x509.validity();
    let names = x509
        .subject_alternative_name()
        .ok()
        .flatten()
        .map(|names| {
            names
                .value
                .general_names
                .iter()
                .filter_map(|name| match name {
                    GeneralName::DNSName(dns) => Some((*dns).to_string()),
                    GeneralName::IPAddress([a, b, c, d]) => Some(format!("{a}.{b}.{c}.{d}")),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();
    Some(Certificate {
        subject: x509.subject().to_string(),
        issuer: x509.issuer().to_string(),
        not_before: DateTime::from_timestamp(validity.not_before.timestamp(), 0)?,
        not_after: DateTime::from_timestamp(validity.not_after.timestamp(), 0)?,
        names,
    })
}
