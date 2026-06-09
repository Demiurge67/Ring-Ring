//! P2P узел на основе libp2p (адаптировано из примера chat)
//! Источник: https://raw.githubusercontent.com/libp2p/rust-libp2p/refs/heads/master/examples/chat/src/main.rs

use libp2p::{
    gossipsub, mdns, noise, tcp, yamux,
    identity,
    Multiaddr, PeerId,
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
    core::upgrade::Version,
    futures::StreamExt,
};
use libp2p_swarm_derive::NetworkBehaviour; 
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;
use log::{info, warn};

/// Поведение узла
#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "MyBehaviourEvent")]
pub struct MyBehaviour {
    gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
}

/// События поведения
#[derive(Debug)]
pub enum MyBehaviourEvent {
    Gossipsub(gossipsub::Event),
    Mdns(mdns::Event),
}

impl From<gossipsub::Event> for MyBehaviourEvent {
    fn from(event: gossipsub::Event) -> Self {
        MyBehaviourEvent::Gossipsub(event)
    }
}

impl From<mdns::Event> for MyBehaviourEvent {
    fn from(event: mdns::Event) -> Self {
        MyBehaviourEvent::Mdns(event)
    }
}

/// Конфигурация узла
pub struct NodeConfig {
    pub listen_addrs: Vec<Multiaddr>,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            listen_addrs: vec!["/ip4/0.0.0.0/tcp/0".parse().unwrap()],
        }
    }
}

/// Основная структура узла
pub struct RingNode {
    pub swarm: Arc<Mutex<Swarm<MyBehaviour>>>,
    pub peer_id: PeerId,
}

impl RingNode {
    pub async fn new(config: NodeConfig) -> Result<Self, Box<dyn Error>> {
        let local_key = identity::Keypair::generate_ed25519();
        let peer_id = PeerId::from(local_key.public());

        let noise_keys = noise::Config::new(&local_key)?;
        let transport = tcp::tokio::Transport::new(tcp::Config::default())
            .upgrade(Version::V1)
            .authenticate(noise_keys)
            .multiplex(yamux::Config::default())
            .boxed();

        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .heartbeat_interval(std::time::Duration::from_secs(10))
            .validation_mode(gossipsub::ValidationMode::Strict)
            .build()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let gossipsub = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(local_key.clone()),
            gossipsub_config,
        )?;

        let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)?;

        let behaviour = MyBehaviour { gossipsub, mdns };

        let mut swarm = Swarm::new(transport, behaviour, peer_id);

        for addr in config.listen_addrs {
            swarm.listen_on(addr)?;
        }

        let topic = gossipsub::IdentTopic::new("ring-ring-chat");
        swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

        let swarm = Arc::new(Mutex::new(swarm));
        let swarm_clone = swarm.clone();

        tokio::spawn(async move {
            let mut swarm = swarm_clone.lock().await;
            loop {
                match swarm.select_next_some().await {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        info!("Listening on {}", address);
                    }
                    SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Discovered(peers))) => {
                        for (peer, addr) in peers {
                            info!("mDNS discovered: {} at {}", peer, addr);
                            swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer);
                        }
                    }
                    SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Expired(peers))) => {
                        for (peer, _) in peers {
                            info!("mDNS expired: {}", peer);
                            swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer);
                        }
                    }
                    SwarmEvent::Behaviour(MyBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                        message, ..
                    })) => {
                        let msg = String::from_utf8_lossy(&message.data);
                        info!("Received: {}", msg);
                    }
                    _ => {}
                }
            }
        });

        Ok(Self { swarm, peer_id })
    }

    pub async fn broadcast(&self, message: String) -> Result<(), Box<dyn Error>> {
        let topic = gossipsub::IdentTopic::new("ring-ring-chat");
        let data = message.as_bytes().to_vec();
        let mut swarm = self.swarm.lock().await;
        swarm.behaviour_mut().gossipsub.publish(topic, data)?;
        Ok(())
    }
}
