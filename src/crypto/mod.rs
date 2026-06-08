//! Cripto primitives

pub mod keypair;
pub mod noise;      // заглушка
pub mod offline;    // заглушка

// Re-export main function
pub use keypair::generate_keypair;
