//! P2P сеть на libp2p

// pub mod node;
pub mod dht;        // заглушка
pub mod behaviour;  // заглушка
pub mod transport;
pub mod discovery;

pub use transport::RingTransport;
pub use discovery::{publish_service, discover_peers};
// pub use node::{RingNode, NodeConfig};
