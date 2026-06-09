//! P2P транспорт на основе QUIC (quinn)

use quinn::{Connection, Endpoint, EndpointConfig, ServerConfig, TransportConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rcgen::{CertificateParams, KeyPair, PKCS_ED25519};
use std::net::SocketAddr;
use std::sync::Arc;
use anyhow::Result;
use log::info;

/// Простой менеджер QUIC-соединений
pub struct RingTransport {
    endpoint: Endpoint,
}

impl RingTransport {
    /// Создаёт транспорт с автоматически сгенерированным сертификатом
    pub async fn new() -> Result<Self> {
        // Генерируем самоподписанный сертификат (для P2P подойдёт)
        let cert = Self::generate_certificate()?;
        let server_config = Self::make_server_config(cert.clone())?;
        let client_config = Self::make_client_config(cert)?;

        let mut endpoint = Endpoint::builder();
        endpoint.with_client_config(client_config);
        endpoint = endpoint.with_server_config(Some(server_config));
        let endpoint = endpoint.bind(&"0.0.0.0:0".parse().unwrap())?;

        info!("QUIC endpoint bound on random port");
        Ok(Self { endpoint })
    }

    /// Привязать endpoint к конкретному адресу (например, для прослушивания)
    pub async fn bind(addr: SocketAddr) -> Result<Self> {
        let cert = Self::generate_certificate()?;
        let server_config = Self::make_server_config(cert.clone())?;
        let client_config = Self::make_client_config(cert)?;

        let mut endpoint = Endpoint::builder();
        endpoint.with_client_config(client_config);
        endpoint = endpoint.with_server_config(Some(server_config));
        let endpoint = endpoint.bind(&addr)?;
        info!("QUIC endpoint listening on {}", addr);
        Ok(Self { endpoint })
    }

    /// Соединиться с удалённым узлом
    pub async fn connect(&self, addr: SocketAddr) -> Result<Connection> {
        let conn = self.endpoint.connect(addr, "localhost")?.await?;
        info!("Connected to {}", addr);
        Ok(conn)
    }

    /// Принять входящее соединение
    pub async fn accept(&self) -> Result<Connection> {
        let incoming = self.endpoint.accept().await.ok_or_else(|| anyhow::anyhow!("no incoming connection"))?;
        let conn = incoming.await?;
        info!("Accepted connection from {}", conn.remote_address());
        Ok(conn)
    }

    /// Генерация сертификата X.509 на основе ed25519
    fn generate_certificate() -> Result<rustls::ServerConfig> {
        let key_pair = KeyPair::generate_for(&PKCS_ED25519)?;
        let params = CertificateParams::new(vec!["localhost".to_string()])?;
        let cert = params.self_signed(&key_pair)?;
        let server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert.cert], PrivateKeyDer::try_from(key_pair.serialize_der())?)?;
        Ok(server_config)
    }

    fn make_server_config(cert: rustls::ServerConfig) -> Result<ServerConfig> {
        let mut server_config = ServerConfig::with_crypto(Arc::new(cert));
        let transport_config = Arc::new(TransportConfig::default());
        server_config.transport_config(transport_config);
        Ok(server_config)
    }

    fn make_client_config(cert: rustls::ServerConfig) -> Result<quinn::ClientConfig> {
        let mut client_config = quinn::ClientConfig::new(Arc::new(cert.clone()));
        let transport_config = Arc::new(TransportConfig::default());
        client_config.transport_config(transport_config);
        Ok(client_config)
    }
}

/// Отправка и получение сообщений по открытому соединению
pub async fn send_message(conn: &mut Connection, data: &[u8]) -> Result<()> {
    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(data).await?;
    send.finish().await?;
    Ok(())
}

pub async fn recv_message(conn: &mut Connection) -> Result<Vec<u8>> {
    let (_, mut recv) = conn.accept_bi().await?;
    let mut buf = Vec::new();
    recv.read_to_end(&mut buf).await?;
    Ok(buf)
}
