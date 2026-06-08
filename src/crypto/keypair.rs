//! Генерация и работа с ключевой парой Ed25519

use ed25519_dalek::{SigningKey, VerifyingKey};
use rand::rngs::OsRng;
use rand::RngCore;

/// Сгенерировать новую пару ключей.
/// Возвращает (закрытый_ключ_в_hex, открытый_ключ_в_hex)
pub fn generate_keypair() -> (String, String) {
    let mut csprng = OsRng;
    let mut secret_bytes = [0u8; 32];
    csprng.fill_bytes(&mut secret_bytes);
    let signing_key = SigningKey::from_bytes(&secret_bytes);
    let verifying_key = signing_key.verifying_key();

    let private_hex = hex::encode(signing_key.to_bytes());
    let public_hex = hex::encode(verifying_key.to_bytes());

    (private_hex, public_hex)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_keypair() {
        let (priv_hex, pub_hex) = generate_keypair();
        assert_eq!(priv_hex.len(), 64);
        assert_eq!(pub_hex.len(), 64);
        assert_ne!(priv_hex, pub_hex);
    }
}
