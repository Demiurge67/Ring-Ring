//! Обнаружение узлов в локальной сети через mDNS

use mdns_sd::{ServiceDaemon, ServiceInfo};
use std::net::SocketAddr;
use anyhow::Result;
use log::info;

const SERVICE_TYPE: &str = "_ring_ring._udp.local.";

/// Запускает mDNS-сервис для текущего узла
pub fn publish_service(instance_name: &str, port: u16) -> Result<ServiceDaemon> {
    let mdns = ServiceDaemon::new()?;
    let service_info = ServiceInfo::new(
        SERVICE_TYPE,
        instance_name,
        &instance_name,
        "",
        port,
        None,
    )?;
    mdns.register(service_info)?;
    info!("mDNS service published: {}", instance_name);
    Ok(mdns)
}

/// Обнаруживает другие узлы в сети, возвращает их сокет-адреса
pub fn discover_peers(timeout_secs: u64) -> Result<Vec<SocketAddr>> {
    let mdns = ServiceDaemon::new()?;
    let receiver = mdns.browse(SERVICE_TYPE)?;
    let mut peers = Vec::new();
    // Ждём события в течение таймаута
    for _ in 0..(timeout_secs * 2) {
        if let Ok(event) = receiver.recv() {
            match event {
                mdns_sd::ServiceEvent::NewResolved(info) => {
                    for addr in info.get_addresses().iter() {
                        let socket = SocketAddr::new(*addr, info.get_port());
                        peers.push(socket);
                    }
                }
                _ => {}
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    Ok(peers)
}
