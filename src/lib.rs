// Ring‑Ring Core Library

pub mod crypto;
pub mod p2p;
pub mod storage;
pub mod events;

// Re-export commonly used types
pub use crypto::{KeyPair, PublicKey, PrivateKey};
pub use p2p::{RingNode, NodeConfig};
pub use storage::Storage;

/// Версия протокола
pub const PROTOCOL_VERSION: &str = "0.1.0-draft";

/// Инициализация библиотеки (логгер, глобальное состояние)
pub fn init() {
    // Простейший логгер (можно заменить на env_logger)
    let _ = env_logger::try_init();
}
