//! P2P сеть на libp2p

//! P2P транспорт через TCP + Noise (XX handshake)
//! Основан на официальном примере snow/simple.rs

use snow::{Builder, TransportState};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use anyhow::{Result, Context};
use std::sync::Arc;
use tokio::sync::Mutex;

const NOISE_PARAMS: &str = "Noise_XX_25519_ChaChaPoly_BLAKE2s";

/// Генерирует статическую ключевую пару для узла (один раз при запуске)
pub fn generate_static_keypair() -> snow::Keypair {
    let mut rng = rand::thread_rng();
    snow::Keypair::generate(&mut rng)
}

/// Сервер: ожидает входящие соединения и для каждого запускает обработчик
pub async fn run_server(
    addr: &str,
    static_key: snow::Keypair,
    on_message: impl Fn(String, std::net::SocketAddr) + Send + Sync + 'static,
) -> Result<()> {
    let listener = TcpListener::bind(addr).await
        .with_context(|| format!("Failed to bind on {}", addr))?;
    println!("Server listening on {}", addr);

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let static_key = static_key.clone();
        let on_message = on_message.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, static_key, true, on_message, peer_addr).await {
                eprintln!("Error handling connection from {}: {}", peer_addr, e);
            }
        });
    }
}

/// Клиент: подключается к удалённому узлу и возвращает объект для отправки сообщений
pub async fn connect_client(
    addr: &str,
    static_key: snow::Keypair,
    on_message: impl Fn(String) + Send + Sync + 'static,
) -> Result<ClientHandle> {
    let stream = TcpStream::connect(addr).await
        .with_context(|| format!("Failed to connect to {}", addr))?;
    let peer_addr = stream.peer_addr()?;
    let (stream, noise) = perform_handshake(stream, static_key, false).await?;
    let stream = Arc::new(Mutex::new(stream));
    let noise = Arc::new(Mutex::new(noise));

    // Фоновый приём сообщений
    let stream_clone = stream.clone();
    let noise_clone = noise.clone();
    tokio::spawn(async move {
        let mut buf = [0u8; 65536];
        loop {
            let n = {
                let mut s = stream_clone.lock().await;
                match s.read(&mut buf).await {
                    Ok(n) => n,
                    Err(_) => break,
                }
            };
            if n == 0 { break; }
            let mut plain = vec![0u8; n];
            let m = {
                let mut noise = noise_clone.lock().await;
                noise.read_message(&buf[..n], &mut plain).unwrap()
            };
            let msg = String::from_utf8_lossy(&plain[..m]).to_string();
            on_message(msg);
        }
    });

    Ok(ClientHandle { stream, noise })
}

/// Обработчик одного соединения (как для сервера, так и для клиента)
async fn handle_connection(
    mut stream: TcpStream,
    static_key: snow::Keypair,
    is_responder: bool,
    on_message: impl Fn(String, std::net::SocketAddr),
    peer_addr: std::net::SocketAddr,
) -> Result<()> {
    let (mut stream, mut noise) = perform_handshake(stream, static_key, is_responder).await?;

    // После handshake — обмен сообщениями
    let mut buf = [0u8; 65536];
    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 { break; }
        let mut plain = vec![0u8; n];
        let m = noise.read_message(&buf[..n], &mut plain)?;
        let msg = String::from_utf8_lossy(&plain[..m]).to_string();
        on_message(msg, peer_addr);
        // Здесь можно отправить ответ, но для простоты пока только приём
    }
    Ok(())
}

async fn perform_handshake(
    mut stream: TcpStream,
    static_key: snow::Keypair,
    is_responder: bool,
) -> Result<(TcpStream, TransportState)> {
    let mut noise = if is_responder {
        Builder::new(NOISE_PARAMS.parse()?)
            .local_private_key(&static_key.private)
            .build_responder()?
    } else {
        Builder::new(NOISE_PARAMS.parse()?)
            .local_private_key(&static_key.private)
            .build_initiator()?
    };

    let mut buf = [0u8; 65536];
    if !is_responder {
        // initiator: -> e
        let len = noise.write_message(&[], &mut buf)?;
        stream.write_all(&buf[..len]).await?;
        // responder: <- e, ee, s, es
        let n = stream.read(&mut buf).await?;
        noise.read_message(&buf[..n], &mut buf)?;
        // initiator: -> s, se
        let len = noise.write_message(&[], &mut buf)?;
        stream.write_all(&buf[..len]).await?;
        let n = stream.read(&mut buf).await?;
        noise.read_message(&buf[..n], &mut buf)?;
    } else {
        // responder: ожидаем -> e
        let n = stream.read(&mut buf).await?;
        noise.read_message(&buf[..n], &mut buf)?;
        // responder: <- e, ee, s, es
        let len = noise.write_message(&[], &mut buf)?;
        stream.write_all(&buf[..len]).await?;
        // responder: ожидаем -> s, se
        let n = stream.read(&mut buf).await?;
        noise.read_message(&buf[..n], &mut buf)?;
    }
    Ok((stream, noise))
}

/// Дескриптор клиентского соединения для отправки сообщений
pub struct ClientHandle {
    stream: Arc<Mutex<TcpStream>>,
    noise: Arc<Mutex<TransportState>>,
}

impl ClientHandle {
    pub async fn send(&self, msg: &str) -> Result<()> {
        let mut out = vec![0u8; msg.len() + 65535];
        let n = {
            let mut noise = self.noise.lock().await;
            noise.write_message(msg.as_bytes(), &mut out)?
        };
        let mut stream = self.stream.lock().await;
        stream.write_all(&out[..n]).await?;
        Ok(())
    }
}
