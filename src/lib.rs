// Ring‑Ring Core Library

pub mod crypto;
pub mod p2p;
pub mod storage;

pub use crypto::keypair::generate_keypair;
pub use storage::keys::{save_keys, load_keys};

/// Версия протокола
pub const PROTOCOL_VERSION: &str = "0.1.0-draft";

/// Инициализация библиотеки (логгер)
pub fn init() {
    let _ = env_logger::try_init();
}

/// Инициализация клиента: загрузка или генерация ключей.
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
            let (priv_hex, pub_hex) = generate_keypair();
            (priv_hex, pub_hex)
        }
    }
}
