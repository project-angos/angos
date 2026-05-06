use std::path::PathBuf;

use reqwest::{Certificate, Identity};

use crate::command::server::Error;

pub fn load_pem_file(path: &PathBuf) -> Result<String, Error> {
    std::fs::read_to_string(path)
        .map_err(|e| Error::Initialization(format!("Failed to read PEM file: {e}")))
}

pub fn load_certificate_bundle(path: &PathBuf) -> Result<Vec<Certificate>, Error> {
    let certificate_pem = load_pem_file(path)?;

    match Certificate::from_pem_bundle(certificate_pem.as_bytes()) {
        Ok(cert) => Ok(cert),
        Err(e) => {
            let msg = format!("Failed to parse certificate: {e}");
            Err(Error::Initialization(msg))
        }
    }
}

pub fn load_identity(
    cert_path: Option<&PathBuf>,
    key_path: Option<&PathBuf>,
) -> Result<Option<Identity>, Error> {
    let (Some(cert_path), Some(key_path)) = (cert_path, key_path) else {
        return Ok(None);
    };

    let cert_pem = load_pem_file(cert_path)?;
    let key_pem = load_pem_file(key_path)?;

    match Identity::from_pem(format!("{cert_pem}{key_pem}").as_bytes()) {
        Ok(identity) => Ok(Some(identity)),
        Err(e) => {
            let msg = format!("Failed to create identity from PEM: {e}");
            Err(Error::Initialization(msg))
        }
    }
}
