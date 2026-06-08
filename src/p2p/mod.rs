//! P2P сеть на libp2p

pub mod node;
pub mod dht;        // заглушка
pub mod behaviour;  // заглушка

pub use node::{RingNode, NodeConfig};
