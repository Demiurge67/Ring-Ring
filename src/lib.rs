// Ring‑Ring Core Library

// Ring‑Ring Core Library

pub mod crypto;
pub mod p2p;
pub mod storage;
pub mod events;

// Re-export основных типов (пока только ключи)
pub use crypto::keypair::generate_keypair;

/// Версия протокола
pub const PROTOCOL_VERSION: &str = "0.1.0-draft";

/// Инициализация библиотеки (логгер, глобальное состояние)
pub fn init() {
    let _ = env_logger::try_init();
}
