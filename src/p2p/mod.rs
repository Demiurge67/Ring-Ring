// examples/simple_chat.rs
// Запуск: cargo run --example simple_chat server
//         cargo run --example simple_chat client
use snow::{Builder, Keypair};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use rand::Rng;
use std::thread;

const PATTERN: &str = "Noise_XX_25519_ChaChaPoly_BLAKE2s";

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let is_server = args.len() > 1 && args[1] == "server";
    let mut rng = rand::thread_rng();
    let mut priv_bytes = [0u8; 32];
    rng.fill_bytes(&mut priv_bytes);
    let static_key = Keypair::from_private(&priv_bytes).unwrap();

    if is_server {
        let listener = TcpListener::bind("0.0.0.0:12345").unwrap();
        println!("Server listening");
        for stream in listener.incoming() {
            let stream = stream.unwrap();
            let key = static_key.clone();
            thread::spawn(move || handle(stream, key, true));
        }
    } else {
        let stream = TcpStream::connect("127.0.0.1:12345").unwrap();
        handle(stream, static_key, false);
    }
}

fn handle(mut stream: TcpStream, key: Keypair, responder: bool) {
    let mut noise = if responder {
        Builder::new(PATTERN.parse().unwrap())
            .local_private_key(&key.private)
            .build_responder()
            .unwrap()
    } else {
        Builder::new(PATTERN.parse().unwrap())
            .local_private_key(&key.private)
            .build_initiator()
            .unwrap()
    };
    let mut buf = [0u8; 65535];
    // Handshake
    if !responder {
        let len = noise.write_message(&[], &mut buf).unwrap();
        stream.write_all(&buf[..len]).unwrap();
        let n = stream.read(&mut buf).unwrap();
        noise.read_message(&buf[..n], &mut buf).unwrap();
        let len = noise.write_message(&[], &mut buf).unwrap();
        stream.write_all(&buf[..len]).unwrap();
        let n = stream.read(&mut buf).unwrap();
        noise.read_message(&buf[..n], &mut buf).unwrap();
    } else {
        let n = stream.read(&mut buf).unwrap();
        noise.read_message(&buf[..n], &mut buf).unwrap();
        let len = noise.write_message(&[], &mut buf).unwrap();
        stream.write_all(&buf[..len]).unwrap();
        let n = stream.read(&mut buf).unwrap();
        noise.read_message(&buf[..n], &mut buf).unwrap();
    }
    println!("Handshake done");
    // Обмен сообщениями: сервер читает и пишет, клиент пишет и читает
    if responder {
        let mut buf = [0u8; 1024];
        loop {
            let n = stream.read(&mut buf).unwrap();
            if n == 0 { break; }
            let mut plain = vec![0u8; n];
            let m = noise.read_message(&buf[..n], &mut plain).unwrap();
            let msg = String::from_utf8(plain[..m].to_vec()).unwrap();
            println!("Received: {}", msg);
        }
    } else {
        let stdin = std::io::stdin();
        loop {
            let mut line = String::new();
            stdin.read_line(&mut line).unwrap();
            let line = line.trim();
            if line.is_empty() { continue; }
            let mut out = vec![0u8; 65535];
            let len = noise.write_message(line.as_bytes(), &mut out).unwrap();
            stream.write_all(&out[..len]).unwrap();
        }
    }
}
