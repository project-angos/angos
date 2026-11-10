use std::io;

use argh::FromArgs;
use argon2::password_hash::Error as PasswordHashError;
use argon2::{Algorithm, Argon2, Params, PasswordHasher, Version};
use zeroize::Zeroizing;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Input(#[from] io::Error),
    #[error("{0}")]
    Hashing(PasswordHashError),
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(
    subcommand,
    name = "argon",
    description = "Hash a password following the argon2id algorithm"
)]
pub struct Options {}

pub fn run() -> Result<(), Error> {
    let password = Zeroizing::new(rpassword::prompt_password("Input Password: ")?);
    let hash = generate_password(&password)?;
    println!("{hash}");
    Ok(())
}

fn generate_password(password: &str) -> Result<String, Error> {
    // OWASP Argon2id minimum (19 MiB, 2 iterations, 1 thread)
    let params = Params::new(19_456, 2, 1, None).map_err(|e| Error::Hashing(e.into()))?;
    let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let hash = argon
        .hash_password(password.as_bytes())
        .map_err(Error::Hashing)?;

    Ok(hash.to_string())
}

#[cfg(test)]
mod tests {
    use argon2::{Argon2, PasswordHash, PasswordVerifier};

    use crate::command::argon::generate_password;

    #[test]
    fn test_generate_password() {
        let password = "my_secure_password";
        let hash = generate_password(password).expect("Failed to generate password hash");

        assert!(
            hash.starts_with("$argon2id$"),
            "hash must start with '$argon2id$', got: {hash}"
        );
        let parsed_hash = PasswordHash::new(&hash).expect("Failed to parse hash");
        let password_verify = Argon2::default().verify_password(password.as_bytes(), &parsed_hash);
        assert!(password_verify.is_ok());
    }
}
