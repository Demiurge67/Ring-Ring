//! Генерация и работа с ключевой парой Ed25519

use ed25519_dalek::{SigningKey, VerifyingKey};
use rand::rngs::OsRng;

/// Сгенерировать новую пару ключей.
/// Возвращает (закрытый_ключ_в_hex, открытый_ключ_в_hex)
pub fn generate_keypair() -> (String, String) {
    let mut csprng = OsRng;
    let signing_key: SigningKey = SigningKey::generate(&mut csprng);
    let verifying_key: VerifyingKey = signing_key.verifying_key();

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
        // Длина закрытого ключа Ed25519: 32 байта -> 64 hex-символа
        assert_eq!(priv_hex.len(), 64);
        // Длина открытого ключа: 32 байта -> 64 hex
        assert_eq!(pub_hex.len(), 64);
        // Они не должны быть одинаковыми
        assert_ne!(priv_hex, pub_hex);
    }
}
