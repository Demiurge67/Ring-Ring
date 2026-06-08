//! Упрощённый P2P узел на TCP + Noise (без libp2p)

use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use noise_protocol::{Noise, Handshake, X25519, ChaChaPoly, HashBlake2b};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::collections::HashMap;
use log::{info, error};

pub type PeerId = String; // временно: строка публичного ключа

pub struct RingNode {
    local_key: noise_protocol::StaticKey,
    public_key_hex: String,
    peers: Arc<Mutex<HashMap<PeerId, TcpStream>>>,
    listener: Option<TcpListener>,
}

impl RingNode {
    /// Создаёт новый узел, генерирует ключи Noise (X25519)
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let local_key = noise_protocol::StaticKey::new();
        let public_key_hex = hex::encode(local_key.public_key());
        Ok(Self {
            local_key,
            public_key_hex,
            peers: Arc::new(Mutex::new(HashMap::new())),
            listener: None,
        })
    }

    /// Запускает прослушивание входящих соединений на указанном адресе (например, "0.0.0.0:12345")
    pub async fn listen(&mut self, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(addr).await?;
        self.listener = Some(listener);
        let peers = self.peers.clone();
        let local_key = self.local_key.clone();
        tokio::spawn(async move {
            let mut listener = listener;
            while let Ok((stream, _)) = listener.accept().await {
                // Запускаем обработку входящего соединения
                tokio::spawn(handle_incoming(stream, peers.clone(), local_key.clone()));
            }
        });
        Ok(())
    }

    /// Устанавливает соединение с удалённым узлом по адресу (ip:port) и выполняет Noise handshake.
    /// Возвращает PeerId (публичный ключ собеседника) и зашифрованный поток.
    pub async fn connect(&self, addr: &str) -> Result<(PeerId, TcpStream), Box<dyn std::error::Error>> {
        let stream = TcpStream::connect(addr).await?;
        let (peer_pubkey, encrypted_stream) = noise_client_handshake(stream, &self.local_key).await?;
        Ok((peer_pubkey, encrypted_stream))
    }

    /// Отправляет сообщение через уже установленный зашифрованный канал.
    pub async fn send(stream: &mut TcpStream, msg: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        let len = msg.len() as u32;
        stream.write_all(&len.to_le_bytes()).await?;
        stream.write_all(msg).await?;
        Ok(())
    }

    /// Получает сообщение из зашифрованного канала.
    pub async fn recv(stream: &mut TcpStream) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await?;
        Ok(buf)
    }

    pub fn public_key_hex(&self) -> &str {
        &self.public_key_hex
    }
}

/// Обработка входящего соединения: выполняем Noise handshake как сервер.
async fn handle_incoming(
    stream: TcpStream,
    peers: Arc<Mutex<HashMap<PeerId, TcpStream>>>,
    local_key: noise_protocol::StaticKey,
) {
    match noise_server_handshake(stream, local_key).await {
        Ok((peer_pubkey, encrypted_stream)) => {
            info!("Handshake successful with peer: {}", hex::encode(&peer_pubkey));
            peers.lock().await.insert(hex::encode(peer_pubkey), encrypted_stream);
        }
        Err(e) => error!("Handshake failed: {}", e),
    }
}

/// Шум-хендшейк со стороны клиента (инициатор)
async fn noise_client_handshake(
    mut stream: TcpStream,
    static_key: &noise_protocol::StaticKey,
) -> Result<(Vec<u8>, TcpStream), Box<dyn std::error::Error>> {
    let mut noise = Noise::new(static_key.clone());
    let mut handshake = Handshake::new(noise, b"Noise_XX_25519_ChaChaPoly_BLAKE2b")?;
    handshake.start_initiator()?;
    // Обмен сообщениями (упрощённо: три пакета)
    let mut buf = [0u8; 1024];
    let len = handshake.write_message(&mut buf)?;
    stream.write_all(&buf[..len]).await?;
    let n = stream.read(&mut buf).await?;
    handshake.read_message(&buf[..n])?;
    let len = handshake.write_message(&mut buf)?;
    stream.write_all(&buf[..len]).await?;
    let n = stream.read(&mut buf).await?;
    handshake.read_message(&buf[..n])?;
    let peer_pubkey = handshake.get_remote_static()?;
    let encrypted_stream = stream;
    Ok((peer_pubkey.to_vec(), encrypted_stream))
}

/// Шум-хендшейк со стороны сервера (ответчик)
async fn noise_server_handshake(
    mut stream: TcpStream,
    static_key: noise_protocol::StaticKey,
) -> Result<(Vec<u8>, TcpStream), Box<dyn std::error::Error>> {
    let mut noise = Noise::new(static_key);
    let mut handshake = Handshake::new(noise, b"Noise_XX_25519_ChaChaPoly_BLAKE2b")?;
    handshake.start_responder()?;
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await?;
    handshake.read_message(&buf[..n])?;
    let len = handshake.write_message(&mut buf)?;
    stream.write_all(&buf[..len]).await?;
    let n = stream.read(&mut buf).await?;
    handshake.read_message(&buf[..n])?;
    let len = handshake.write_message(&mut buf)?;
    stream.write_all(&buf[..len]).await?;
    let peer_pubkey = handshake.get_remote_static()?;
    let encrypted_stream = stream;
    Ok((peer_pubkey.to_vec(), encrypted_stream))
}
