use janus_core::Protocol;
use serde::Deserialize;
use std::{fs, net::SocketAddr};

pub fn janus_config() -> &'static str {
    "janus-config"
}

// The configuration can define multiple services on different ports.
#[derive(Debug, Deserialize)]
pub struct JanusConfig {
    pub services: Vec<ServiceConfig>,
}

// One service can pool multiple backends
#[derive(Debug, Deserialize)]
pub struct ServiceConfig {
    pub name: String,
    pub listen_addr: SocketAddr,
    pub backends: Vec<Backend>,
    pub protocol: Protocol,
}

#[derive(Debug, Deserialize)]
pub struct Backend { // Simple types because that's what serde reads straight out of the file
    pub id: String,
    pub address: SocketAddr,
    pub weight: u32,
}

// Load and parse the Janus configuration file into Rust structs.
pub fn load_config(path: &str) -> janus_core::Result<JanusConfig> {
    let content: String = fs::read_to_string(path)?;
    let config = toml::from_str::<JanusConfig>(&content)
        .map_err(|error| janus_core::Error::Config(error.to_string()))?;
    Ok(config)
}
