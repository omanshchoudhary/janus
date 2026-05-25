use std::net::SocketAddr;

pub fn janus_config() -> &'static str {
    "janus-config"
}

pub struct ServiceConfig {
    pub name: String,
    pub listen_addr: SocketAddr,
    pub backends: Vec<BackendConfig>,
}

pub struct BackendConfig {
    pub id: String,
    pub address: SocketAddr,
    pub weight: u32,
}
