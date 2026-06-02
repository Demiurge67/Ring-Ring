//! Cripto primitives

pub mod keypair;
pub mod noise;      // пока заглушка
pub mod offline;    // пока заглушка

// Re-export главных функций
pub use keypair::generate_keypair;
