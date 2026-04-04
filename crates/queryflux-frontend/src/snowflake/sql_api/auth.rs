//! Service-account JWT generation for the Snowflake SQL REST API v2.
//!
//! Snowflake SQL API v2 uses key-pair authentication: the caller signs a JWT
//! with their RSA private key and presents it as `Authorization: Bearer {jwt}`.
//!
//! JWT claims (per Snowflake docs):
//!   iss: "{account}.{user}.SHA256:{public_key_fingerprint}"
//!   sub: "{account}.{user}"
//!   iat: <now_unix>
//!   exp: <now_unix + 60>
//!
//! The `public_key_fingerprint` is the SHA256 hash of the SPKI DER-encoded public key,
//! base64-encoded without padding.

use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rsa::pkcs8::{DecodePrivateKey, EncodePublicKey};
use rsa::RsaPrivateKey;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Serialize, Deserialize)]
struct SnowflakeJwtClaims {
    iss: String,
    sub: String,
    iat: i64,
    exp: i64,
}

/// Generate a Snowflake SQL API v2 service-account JWT from the cluster's key-pair credentials.
///
/// - `account`: Snowflake account identifier (e.g. `"xy12345"`)
/// - `username`: Service-account username (e.g. `"QUERYFLUX_SA"`)
/// - `private_key_pem`: PKCS#8 PEM-encoded RSA private key
pub fn generate_service_account_jwt(
    account: &str,
    username: &str,
    private_key_pem: &str,
) -> Result<String, String> {
    let private_key = RsaPrivateKey::from_pkcs8_pem(private_key_pem)
        .map_err(|e| format!("Failed to parse RSA private key: {e}"))?;

    let public_key = private_key.to_public_key();
    let public_key_der = public_key
        .to_public_key_der()
        .map_err(|e| format!("Failed to encode public key as DER: {e}"))?;

    // SHA256 fingerprint of the SPKI DER public key, base64-encoded (no padding).
    let fingerprint = {
        let hash = Sha256::digest(public_key_der.as_bytes());
        use base64::Engine;
        base64::engine::general_purpose::STANDARD_NO_PAD.encode(hash)
    };

    let now = chrono::Utc::now().timestamp();
    let account_upper = account.to_uppercase();
    let user_upper = username.to_uppercase();

    let claims = SnowflakeJwtClaims {
        iss: format!("{account_upper}.{user_upper}.SHA256:{fingerprint}"),
        sub: format!("{account_upper}.{user_upper}"),
        iat: now,
        exp: now + 60,
    };

    let encoding_key = EncodingKey::from_rsa_pem(private_key_pem.as_bytes())
        .map_err(|e| format!("Failed to build encoding key: {e}"))?;

    encode(&Header::new(Algorithm::RS256), &claims, &encoding_key)
        .map_err(|e| format!("JWT encoding failed: {e}"))
}
