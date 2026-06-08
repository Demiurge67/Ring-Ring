//! P2P узел на основе libp2p (адаптировано из примера chat)
//! Источник: https://raw.githubusercontent.com/libp2p/rust-libp2p/refs/heads/master/examples/chat/src/main.rs

use libp2p::{
    gossipsub,
    mdns,
    noise,
    swarm::SwarmBuilder,
    tcp,
    yamux,
    Multiaddr, PeerId,
    identity,
    swarm::{NetworkBehaviour, SwarmEvent},
};
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;
use futures::StreamExt;
use log::{info, error};

/// Поведение узла: gossipsub (чат) + mDNS (обнаружение)
#[derive(NetworkBehaviour)]
pub struct MyBehaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
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

/// Основная структура P2P узла
pub struct RingNode {
    pub swarm: Arc<Mutex<libp2p::Swarm<MyBehaviour>>>,
    pub peer_id: PeerId,
}

impl RingNode {
    pub async fn new(config: NodeConfig) -> Result<Self, Box<dyn Error>> {
        // Генерируем ключи (в будущем заменим на наши из crypto)
        let local_key = identity::Keypair::generate_ed25519();
        let peer_id = PeerId::from(local_key.public());

        // Транспорт: TCP, Noise, Yamux
        let transport = tcp::tokio::Transport::new(tcp::Config::default())
            .upgrade(libp2p::core::upgrade::Version::V1)
            .authenticate(noise::Config::new(&local_key)?)
            .multiplex(yamux::Config::default())
            .boxed();

        // Настройка gossipsub
        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .heartbeat_interval(std::time::Duration::from_secs(10))
            .validation_mode(gossipsub::ValidationMode::Strict)
            .build()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let gossipsub = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(local_key.clone()),
            gossipsub_config,
        )?;

        // mDNS для локального обнаружения
        let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)?;

        let behaviour = MyBehaviour { gossipsub, mdns };
        let mut swarm = SwarmBuilder::with_tokio_executor(transport, behaviour, peer_id).build();

        // Запускаем прослушивание адресов
        for addr in config.listen_addrs {
            swarm.listen_on(addr)?;
        }

        // Подписываемся на топик (например, "ring-ring-chat")
        let topic = gossipsub::IdentTopic::new("ring-ring-chat");
        swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

        let swarm = Arc::new(Mutex::new(swarm));
        let swarm_clone = swarm.clone();

        // Запускаем обработку событий
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
                        propagation_source: _,
                        message_id: _,
                        message,
                    })) => {
                        let msg = String::from_utf8_lossy(&message.data);
                        info!("Received: {}", msg);
                        // TODO: расшифровать сообщение и сохранить в хранилище
                    }
                    _ => {}
                }
            }
        });

        Ok(Self { swarm, peer_id })
    }

    /// Отправить сообщение в топик всем подписчикам
    pub async fn broadcast(&self, message: String) -> Result<(), Box<dyn Error>> {
        let topic = gossipsub::IdentTopic::new("ring-ring-chat");
        let data = message.as_bytes().to_vec();
        let mut swarm = self.swarm.lock().await;
        swarm.behaviour_mut().gossipsub.publish(topic, data)?;
        Ok(())
    }
}

// Тип событий для поведения
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
