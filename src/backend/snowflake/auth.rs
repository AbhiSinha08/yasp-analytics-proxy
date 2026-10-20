//! RSA key-pair JWT authentication for the public SQL API.
use super::SnowflakeConfig;
use base64::{Engine, engine::general_purpose::STANDARD};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rsa::{
    RsaPrivateKey,
    pkcs1::EncodeRsaPrivateKey,
    pkcs8::{DecodePrivateKey, EncodePublicKey},
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    io::{self, Read},
    time::{SystemTime, UNIX_EPOCH},
};

pub(super) struct KeyPair {
    key: EncodingKey,
    subject: String,
    issuer: String,
}
impl KeyPair {
    pub(super) fn load(config: &SnowflakeConfig) -> io::Result<Self> {
        let invalid = || {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot read Snowflake PKCS#8 RSA private key",
            )
        };
        let mut pem = String::new();
        std::fs::File::open(&config.private_key_file)?
            .take(32 * 1024 + 1)
            .read_to_string(&mut pem)?;
        if pem.len() > 32 * 1024 {
            return Err(invalid());
        }
        let private = match &config.private_key_passphrase {
            Some(password) => RsaPrivateKey::from_pkcs8_encrypted_pem(&pem, password),
            None => RsaPrivateKey::from_pkcs8_pem(&pem),
        }
        .map_err(|_| invalid())?;
        use rsa::traits::PublicKeyParts;
        if !(2048..=4096).contains(&private.n().bits()) {
            return Err(invalid());
        }
        let public = private
            .to_public_key()
            .to_public_key_der()
            .map_err(|_| invalid())?;
        let fingerprint = STANDARD.encode(Sha256::digest(public.as_bytes()));
        let key = private.to_pkcs1_der().map_err(|_| invalid())?;
        // Locator region suffixes are excluded; organization-account identifiers
        // contain no dots. Global account identifiers replace dots with hyphens.
        let account = if config.account.to_ascii_lowercase().contains(".global") {
            config.account.replace('.', "-")
        } else {
            config.account.split('.').next().unwrap().to_owned()
        };
        let subject = format!(
            "{}.{}",
            account.to_uppercase(),
            config.username.to_uppercase()
        );
        Ok(Self {
            key: EncodingKey::from_rsa_der(key.as_bytes()),
            issuer: format!("{subject}.SHA256:{fingerprint}"),
            subject,
        })
    }
    pub(super) fn token(&self) -> Result<String, jsonwebtoken::errors::Error> {
        #[derive(Serialize)]
        struct Claims<'a> {
            iss: &'a str,
            sub: &'a str,
            iat: u64,
            exp: u64,
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        jsonwebtoken::encode(
            &Header::new(Algorithm::RS256),
            &Claims {
                iss: &self.issuer,
                sub: &self.subject,
                iat: now,
                exp: now + 300,
            },
            &self.key,
        )
    }
}
