//! P2P транспорт на TCP + Noise (XX handshake) – блокирующий ввод-вывод в отдельном потоке

use snow::{Builder, Keypair, TransportState};
use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};
use anyhow::{Result, Context};
use std::sync::Arc;
use tokio::sync::Mutex;
use rand::RngCore;

const NOISE_PARAMS: &str = "Noise_XX_25519_ChaChaPoly_BLAKE2s";

/// Генерирует статическую ключевую пару для узла (один раз при запуске)
pub fn generate_static_keypair() -> Keypair {
    let mut rng = rand::thread_rng();
    Keypair::generate(&mut rng)
}

/// Запускает сервер в отдельном потоке
pub async fn run_server(
    addr: &str,
    static_key: Keypair,
    on_message: impl Fn(String, std::net::SocketAddr) + Send + Sync + 'static,
) -> Result<()> {
    let addr = addr.to_string();
    tokio::task::spawn_blocking(move || {
        let listener = TcpListener::bind(&addr)
            .with_context(|| format!("Failed to bind on {}", addr))?;
        println!("Server listening on {}", addr);
        for stream in listener.incoming() {
            let stream = stream?;
            let peer_addr = stream.peer_addr()?;
            let static_key = static_key.clone();
            let on_message = on_message.clone();
            std::thread::spawn(move || {
                if let Err(e) = handle_connection(stream, static_key, true, on_message, peer_addr) {
                    eprintln!("Error: {}", e);
                }
            });
        }
        Ok::<_, anyhow::Error>(())
    }).await??;
    Ok(())
}

/// Подключается к серверу и возвращает дескриптор для отправки сообщений
pub async fn connect_client(
    addr: &str,
    static_key: Keypair,
    on_message: impl Fn(String) + Send + Sync + 'static,
) -> Result<ClientHandle> {
    let addr = addr.to_string();
    let (stream, noise) = tokio::task::spawn_blocking(move || {
        let stream = TcpStream::connect(&addr)
            .with_context(|| format!("Failed to connect to {}", addr))?;
        let (stream, noise) = perform_handshake(stream, static_key, false)?;
        Ok::<_, anyhow::Error>((stream, noise))
    }).await??;
    let stream = Arc::new(Mutex::new(stream));
    let noise = Arc::new(Mutex::new(noise));
    let on_message = Arc::new(on_message);

    let stream_clone = stream.clone();
    let noise_clone = noise.clone();
    let on_message_clone = on_message.clone();
    tokio::task::spawn_blocking(move || {
        let mut in_buf = [0u8; 65536];
        loop {
            let n = {
                let mut s = stream_clone.blocking_lock();
                match s.read(&mut in_buf) {
                    Ok(n) => n,
                    Err(_) => break,
                }
            };
            if n == 0 { break; }
            let mut out_buf = vec![0u8; n];
            let m = {
                let mut noise = noise_clone.blocking_lock();
                noise.read_message(&in_buf[..n], &mut out_buf).unwrap()
            };
            let msg = String::from_utf8_lossy(&out_buf[..m]).to_string();
            on_message_clone(msg);
        }
    });

    Ok(ClientHandle { stream, noise })
}

/// Обработка одного соединения (серверная сторона)
fn handle_connection(
    mut stream: TcpStream,
    static_key: Keypair,
    is_responder: bool,
    on_message: impl Fn(String, std::net::SocketAddr),
    peer_addr: std::net::SocketAddr,
) -> Result<()> {
    let (mut stream, mut noise) = perform_handshake(stream, static_key, is_responder)?;
    let mut in_buf = [0u8; 65536];
    loop {
        let n = stream.read(&mut in_buf)?;
        if n == 0 { break; }
        let mut out_buf = vec![0u8; n];
        let m = noise.read_message(&in_buf[..n], &mut out_buf)?;
        let msg = String::from_utf8_lossy(&out_buf[..m]).to_string();
        on_message(msg, peer_addr);
    }
    Ok(())
}

/// Выполняет Noise handshake (полностью синхронно, по паттерну XX)
fn perform_handshake(
    mut stream: TcpStream,
    static_key: Keypair,
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

    let mut in_buf = [0u8; 65536];
    let mut out_buf = [0u8; 65536];

    if !is_responder {
        // Инициатор: -> e
        let len = noise.write_message(&[], &mut out_buf)?;
        stream.write_all(&out_buf[..len])?;
        // <- e, ee, s, es
        let n = stream.read(&mut in_buf)?;
        let len = noise.read_message(&in_buf[..n], &mut out_buf)?;
        // -> s, se
        let len = noise.write_message(&[], &mut out_buf)?;
        stream.write_all(&out_buf[..len])?;
        // <- (финал)
        let n = stream.read(&mut in_buf)?;
        noise.read_message(&in_buf[..n], &mut out_buf)?;
    } else {
        // Респондент: <- e
        let n = stream.read(&mut in_buf)?;
        noise.read_message(&in_buf[..n], &mut out_buf)?;
        // -> e, ee, s, es
        let len = noise.write_message(&[], &mut out_buf)?;
        stream.write_all(&out_buf[..len])?;
        // <- s, se
        let n = stream.read(&mut in_buf)?;
        noise.read_message(&in_buf[..n], &mut out_buf)?;
    }
    Ok((stream, noise))
}

/// Дескриптор клиента для отправки сообщений
pub struct ClientHandle {
    stream: Arc<Mutex<TcpStream>>,
    noise: Arc<Mutex<TransportState>>,
}

impl ClientHandle {
    pub async fn send(&self, msg: &str) -> Result<()> {
        let mut out_buf = vec![0u8; msg.len() + 65535];
        let n = {
            let mut noise = self.noise.lock().await;
            noise.write_message(msg.as_bytes(), &mut out_buf)?
        };
        let mut stream = self.stream.lock().await;
        stream.write_all(&out_buf[..n])?;
        Ok(())
    }
}
