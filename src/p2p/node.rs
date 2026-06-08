//! P2P узел на основе libp2p

use libp2p::{
    identity::Keypair,
    swarm::{Swarm, SwarmBuilder},
    Multiaddr, PeerId,
};
use libp2p::ping::{Ping, Config as PingConfig};
use libp2p::tcp::tokio::Transport as TcpTransport;
use libp2p::dns::tokio::Transport as DnsTransport;
use libp2p::noise::Config as NoiseConfig;
use libp2p::yamux::Config as YamuxConfig;
use libp2p::core::transport::OrTransport;
use libp2p::core::upgrade::Version;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;
use log::info;

/// Настройки узла
#[derive(Clone)]
pub struct NodeConfig {
    /// Список адресов для прослушивания (например, "/ip4/0.0.0.0/tcp/0")
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
    swarm: Arc<Mutex<Swarm<Ping>>>,
    peer_id: PeerId,
}

impl RingNode {
    /// Создаёт и запускает новый узел с заданной конфигурацией
    pub async fn new(config: NodeConfig) -> Result<Self, Box<dyn Error>> {
        // Генерация локальной ключевой пары libp2p (для простоты - новая)
        // В будущем: использовать ключи Ed25519 из crypto модуля
        let local_key = Keypair::generate_ed25519();
        let peer_id = PeerId::from(local_key.public());

        // Транспорт: TCP с DNS и Noise + Yamux
        let transport = {
            let tcp = TcpTransport::new(libp2p::tcp::Config::default());
            let dns_tcp = DnsTransport::system(tcp)?;
            let noise_config = NoiseConfig::xx(&local_key)?;
            dns_tcp
                .upgrade(Version::V1)
                .authenticate(noise_config)
                .multiplex(YamuxConfig::default())
                .boxed()
        };

        // Поведение: только Ping для начала
        let behaviour = Ping::new(PingConfig::new());

        // Строим Swarm
        let mut swarm = SwarmBuilder::with_tokio_executor(transport, behaviour, peer_id).build();

        // Прослушивание адресов
        for addr in config.listen_addrs {
            swarm.listen_on(addr)?;
        }

        // Запускаем фоновую задачу обработки событий
        let swarm = Arc::new(Mutex::new(swarm));
        let swarm_clone = swarm.clone();
        tokio::spawn(async move {
            let mut swarm = swarm_clone.lock().await;
            loop {
                if let Err(e) = swarm.select_next_some().await {
                    log::error!("Swarm error: {}", e);
                }
            }
        });

        Ok(Self { swarm, peer_id })
    }

    /// Возвращает PeerId узла
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// Возвращает список адресов, на которых узел слушает
    pub async fn listen_addrs(&self) -> Vec<Multiaddr> {
        self.swarm.lock().await.listeners().cloned().collect()
    }

    /// Останавливает узел (пока просто заглушка, в будущем graceful shutdown)
    pub async fn stop(&self) {
        // TODO: graceful shutdown
        info!("Stopping RingNode");
    }
}
