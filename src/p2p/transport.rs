//! P2P транспорт на QUIC (quinn)

use quinn::{Endpoint, ServerConfig, ClientConfig, Connection, TransportConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rcgen::{CertificateParams, KeyPair, PKCS_ED25519};
use std::net::SocketAddr;
use std::sync::Arc;
use anyhow::Result;
use log::info;

/// Создаёт серверную конфигурацию с самоподписанным сертификатом
fn make_server_config() -> Result<ServerConfig> {
    let key_pair = KeyPair::generate(&PKCS_ED25519)?;
    let mut params = CertificateParams::new(vec!["localhost".to_string()])?;
    params.is_ca = rcgen::IsCa::NoCa;
    let cert = params.self_signed(&key_pair)?;
    let cert_der = CertificateDer::from(cert.serialize_der()?);
    let key_der = PrivateKeyDer::try_from(key_pair.serialize_der())?;
    let mut server_config = ServerConfig::with_single_cert(vec![cert_der], key_der)?;
    let transport_config = TransportConfig::default();
    server_config.transport_config(Arc::new(transport_config));
    Ok(server_config)
}

/// Создаёт клиентскую конфигурацию, доверяющую любому сертификату (учебный режим)
fn make_client_config() -> Result<ClientConfig> {
    let roots = rustls::RootCertStore::empty();
    let mut client_config = ClientConfig::new(Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ));
    let transport_config = TransportConfig::default();
    client_config.transport_config(Arc::new(transport_config));
    Ok(client_config)
}

/// Основной транспорт: объединяет серверную и клиентскую части
pub struct RingTransport {
    endpoint: Endpoint,
}

impl RingTransport {
    /// Создаёт endpoint, слушающий на заданном адресе и способный к исходящим соединениям
    pub async fn bind(addr: SocketAddr) -> Result<Self> {
        let server_config = make_server_config()?;
        let mut endpoint = Endpoint::server(server_config, addr)?;
        let client_config = make_client_config()?;
        endpoint.set_default_client_config(client_config);
        info!("QUIC endpoint listening on {}", addr);
        Ok(Self { endpoint })
    }

    /// Устанавливает исходящее соединение с удалённым узлом
    pub async fn connect(&self, addr: SocketAddr) -> Result<Connection> {
        let conn = self.endpoint.connect(addr, "localhost")?.await?;
        Ok(conn)
    }

    /// Принимает входящее соединение
    pub async fn accept(&self) -> Result<Connection> {
        let incoming = self.endpoint.accept().await.ok_or_else(|| anyhow::anyhow!("no incoming connection"))?;
        let conn = incoming.await?;
        Ok(conn)
    }
}

/// Отправляет данные по установленному соединению (открывает двусторонний поток)
pub async fn send_message(conn: &mut Connection, data: &[u8]) -> Result<()> {
    let (mut send, _) = conn.open_bi().await?;
    send.write_all(data).await?;
    send.finish().await?;
    Ok(())
}

/// Получает данные из соединения
pub async fn recv_message(conn: &mut Connection) -> Result<Vec<u8>> {
    let (_, mut recv) = conn.accept_bi().await?;
    let mut buf = Vec::new();
    recv.read_to_end(&mut buf).await?;
    Ok(buf)
}
