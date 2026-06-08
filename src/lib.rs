// Ring‑Ring Core Library

pub mod crypto;
pub mod p2p;
pub mod storage;
pub mod events;

pub use crypto::keypair::generate_keypair;
pub use storage::keys::{save_keys, load_keys};

/// Версия протокола
pub const PROTOCOL_VERSION: &str = "0.1.0-draft";

/// Инициализация библиотеки (логгер)
pub fn init() {
    let _ = env_logger::try_init();
}

/// Инициализация клиента: загрузка или генерация ключей.
/// Возвращает (private_key_hex, public_key_hex)
pub fn init_client() -> (String, String) {
    init();
    match load_keys() {
        Ok(Some((priv_hex, pub_hex))) => {
            log::info!("Keys loaded from storage");
            (priv_hex, pub_hex)
        }
        Ok(None) => {
            log::info!("No keys found, generating new keypair");
            let (priv_hex, pub_hex) = generate_keypair();
            if let Err(e) = save_keys(&priv_hex, &pub_hex) {
                log::error!("Failed to save keys: {}", e);
            }
            (priv_hex, pub_hex)
        }
        Err(e) => {
            log::error!("Failed to load keys: {}", e);
            // В случае ошибки загрузки генерируем новые (без сохранения?)
            let (priv_hex, pub_hex) = generate_keypair();
            (priv_hex, pub_hex)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_client() {
        let (priv_hex, pub_hex) = init_client();
        assert_eq!(priv_hex.len(), 64);
        assert_eq!(pub_hex.len(), 64);
    }
}
