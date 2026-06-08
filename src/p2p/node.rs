//! P2P узел с Kademlia DHT (libp2p 0.53)

use libp2p::{
    identity::Keypair,
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
    Multiaddr, PeerId,
};
use libp2p::kad::{Kademlia, KademliaEvent, store::MemoryStore, record::Key};
use libp2p::mdns::{Mdns, MdnsEvent};
use libp2p::ping::{Ping, PingEvent};
use libp2p::tcp::tokio::Transport as TcpTransport;
use libp2p::dns::tokio::Transport as DnsTransport;
use libp2p::noise::Config as NoiseConfig;
use libp2p::yamux::Config as YamuxConfig;
use libp2p::core::Transport;
use libp2p::core::upgrade::Version;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task;
use log::{info, warn};

/// Поведение, объединяющее Kademlia, mDNS и Ping
#[derive(NetworkBehaviour)]
#[behaviour(out_event = "MyBehaviourEvent")]
pub struct MyBehaviour {
    kademlia: Kademlia<MemoryStore>,
    mdns: Mdns,
    ping: Ping,
}

#[derive(Debug)]
pub enum MyBehaviourEvent {
    Kademlia(KademliaEvent),
    Mdns(MdnsEvent),
    Ping(PingEvent),
}

impl From<KademliaEvent> for MyBehaviourEvent {
    fn from(event: KademliaEvent) -> Self {
        MyBehaviourEvent::Kademlia(event)
    }
}

impl From<MdnsEvent> for MyBehaviourEvent {
    fn from(event: MdnsEvent) -> Self {
        MyBehaviourEvent::Mdns(event)
    }
}

impl From<PingEvent> for MyBehaviourEvent {
    fn from(event: PingEvent) -> Self {
        MyBehaviourEvent::Ping(event)
    }
}

/// Настройки узла
#[derive(Clone)]
pub struct NodeConfig {
    pub listen_addrs: Vec<Multiaddr>,
    pub bootstrap_nodes: Vec<Multiaddr>,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            listen_addrs: vec!["/ip4/0.0.0.0/tcp/0".parse().unwrap()],
            bootstrap_nodes: vec![],
        }
    }
}

/// Основная структура P2P узла
pub struct RingNode {
    swarm: Arc<Mutex<Swarm<MyBehaviour>>>,
    peer_id: PeerId,
}

impl RingNode {
    pub async fn new(config: NodeConfig) -> Result<Self, Box<dyn Error>> {
        // Генерация ключей libp2p (в будущем можно заменить на наши из crypto)
        let local_key = Keypair::generate_ed25519();
        let peer_id = PeerId::from(local_key.public());

        // Построение транспорта: DNS over TCP + Noise + Yamux
        let transport = {
            let tcp = TcpTransport::new(libp2p::tcp::Config::default());
            let dns_tcp = DnsTransport::system(tcp)?;
            let noise_config = NoiseConfig::new(&local_key)?;
            dns_tcp
                .upgrade(Version::V1)
                .authenticate(noise_config)
                .multiplex(YamuxConfig::default())
                .boxed()
        };

        // Kademlia с хранилищем в памяти
        let store = MemoryStore::new(peer_id);
        let kademlia = Kademlia::new(peer_id, store);

        // mDNS для локального обнаружения
        let mdns = Mdns::new()?;

        let ping = Ping::default();

        let behaviour = MyBehaviour { kademlia, mdns, ping };

        let mut swarm = Swarm::new(transport, behaviour, peer_id);

        for addr in config.listen_addrs {
            swarm.listen_on(addr)?;
        }

        // Подключение к bootstrap нодам
        for addr in config.bootstrap_nodes {
            if let Some(peer_id) = extract_peer_id(&addr) {
                swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
                // Инициируем bootstrap (поиск других узлов)
                let _ = swarm.behaviour_mut().kademlia.bootstrap();
            } else {
                warn!("Bootstrap address without peer_id: {}", addr);
            }
        }

        let swarm = Arc::new(Mutex::new(swarm));
        let swarm_clone = swarm.clone();

        // Запуск обработки событий в отдельной задаче
        task::spawn(async move {
            let mut swarm = swarm_clone.lock().await;
            loop {
                match swarm.select_next_some().await {
                    SwarmEvent::Behaviour(MyBehaviourEvent::Kademlia(event)) => {
                        info!("Kademlia event: {:?}", event);
                    }
                    SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(MdnsEvent::Discovered(list))) => {
                        for (peer, addr) in list {
                            info!("mDNS discovered: {} at {}", peer, addr);
                            swarm.behaviour_mut().kademlia.add_address(&peer, addr);
                        }
                    }
                    SwarmEvent::Behaviour(MyBehaviourEvent::Ping(_)) => {}
                    SwarmEvent::NewListenAddr { address, .. } => {
                        info!("Listening on {}", address);
                    }
                    _ => {}
                }
            }
        });

        Ok(Self { swarm, peer_id })
    }

    /// Публикует запись в DHT: ключ = произвольная строка (например, hex публичного ключа пользователя),
    /// значение = мультиадрес узла (в виде строки)
    pub async fn publish_address(&self, key_str: &str, address: Multiaddr) -> Result<(), Box<dyn Error>> {
        let key = Key::new(key_str.as_bytes().to_vec());
        let value = address.to_string().into_bytes();
        // В libp2p 0.53 запись не требует подписи для памяти, просто кладём в DHT
        self.swarm.lock().await.behaviour_mut().kademlia.put_record(key, value, libp2p::kad::Quorum::One)?;
        info!("Published address for key {}", key_str);
        Ok(())
    }

    // Для поиска адреса по ключу нужна асинхронная логика с каналами. Пока заглушка.
    // В следующем шаге реализуем полноценный lookup.

    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    pub async fn listen_addrs(&self) -> Vec<Multiaddr> {
        self.swarm.lock().await.listeners().cloned().collect()
    }

    pub async fn stop(&self) {
        info!("Stopping RingNode");
    }
}

/// Извлечь PeerId из мультиадреса (если присутствует компонент /p2p/...)
fn extract_peer_id(addr: &Multiaddr) -> Option<PeerId> {
    let mut iter = addr.iter();
    while let Some(protocol) = iter.next() {
        if let libp2p::core::multiaddr::Protocol::P2p(hash) = protocol {
            return Some(PeerId::from_multihash(hash).ok()?);
        }
    }
    None
}
