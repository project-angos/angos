use std::path::PathBuf;

use reqwest::{Certificate, Identity};

use crate::command::server::Error;

pub fn load_file(path: &PathBuf) -> Result<Vec<u8>, Error> {
    match std::fs::read(path) {
        Ok(pem) => Ok(pem),
        Err(e) => {
            let msg = format!("Failed to read certificate file: {e}");
            Err(Error::Initialization(msg))
        }
    }
}

pub fn load_certificate_bundle(path: &PathBuf) -> Result<Vec<Certificate>, Error> {
    let certificate_pem = load_file(path)?;

    match Certificate::from_pem_bundle(&certificate_pem) {
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

    let cert_pem = load_file(cert_path)?;
    let key_pem = load_file(key_path)?;

    match Identity::from_pem(&[cert_pem, key_pem].concat()) {
        Ok(identity) => Ok(Some(identity)),
        Err(e) => {
            let msg = format!("Failed to create identity from PEM: {e}");
            Err(Error::Initialization(msg))
        }
    }
}
