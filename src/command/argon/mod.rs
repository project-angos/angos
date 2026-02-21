mod error;

use argh::FromArgs;
use argon2::{
    Algorithm, Argon2, Params, PasswordHasher, Version,
    password_hash::{SaltString, rand_core::OsRng},
};
use zeroize::Zeroizing;

#[derive(FromArgs, PartialEq, Debug)]
#[allow(clippy::struct_excessive_bools)]
#[argh(
    subcommand,
    name = "argon",
    description = "Hash a password following the argon2id algorithm"
)]
pub struct Options {}

pub struct Command {}

impl Command {
    pub fn run() -> Result<(), error::Error> {
        let password = Zeroizing::new(rpassword::prompt_password("Input Password: ")?);
        let hash = generate_password(&password)?;
        println!("{hash}");
        Ok(())
    }
}

fn generate_password(password: &str) -> Result<String, error::Error> {
    let salt = SaltString::generate(OsRng);

    let config = Params::default();
    let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, config);
    let hash = argon.hash_password(password.as_bytes(), &salt)?;

    Ok(hash.to_string())
}

#[cfg(test)]
mod tests {
    use argon2::{Argon2, PasswordVerifier};

    use crate::command::argon::generate_password;

    #[test]
    fn test_generate_password() {
        let password = "my_secure_password";
        let hash = generate_password(password).expect("Failed to generate password hash");

        let parsed_hash = argon2::PasswordHash::new(&hash).expect("Failed to parse hash");
        let password_verify = Argon2::default().verify_password(password.as_bytes(), &parsed_hash);
        assert!(password_verify.is_ok());
    }
}
