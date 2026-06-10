#![deny(clippy::all)]
#![warn(clippy::pedantic)]

use std::env;

use janus_config::load_config;
use janus_core::{Backend, BackendAddress, BackendId};
use janus_proxy::ListenerConfig;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    if let Err(error) = run().await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

// Execution Starts Here
async fn run() -> janus_core::Result<()> {
    // Collecting arguements from cli
    let args: Vec<String> = env::args().collect();

    match parse_args(&args) {
        // Use the config file path provided by the CLI.
        Ok(config_path) => {
            tracing::info!("Janus starting with config: {}", config_path);
            // Read the configuration file.
            let config = load_config(&config_path)?;

            // Select the first service.
            let service = config
                .services
                .first()
                .ok_or_else(|| janus_core::Error::Config("no services defined".to_string()))?;

            // Select the first backend of the first service.
            let backend_config = service
                .backends
                .first()
                .ok_or_else(|| janus_core::Error::Config("no backends defined".to_string()))?;

            // Build the runtime listener configuration.
            let listener_config = ListenerConfig {
                listen_addr: service.listen_addr,
            };

            // Build the runtime backend model.
            let backend = Backend {
                id: BackendId(backend_config.id.clone()),
                address: BackendAddress(backend_config.address),
                weight: backend_config.weight,
            };

            janus_proxy::run_tcp_listener(listener_config, backend).await?;
            Ok(())
        }
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    }
}

fn parse_args(args: &[String]) -> Result<String, String> {
    let mut config_path = None;
    let mut iter = args.iter().skip(1);

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                print_help();
                std::process::exit(0)
            }
            "-c" | "--config" => {
                if let Some(path) = iter.next() {
                    config_path = Some(path.clone());
                } else {
                    return Err("Error: Missing value for --config flag".to_string());
                }
            }
            other => {
                return Err(format!(
                    "Error: Unknown argument '{}'\nUse --help for usage details.",
                    other
                ));
            }
        }
    }
    config_path.ok_or_else(|| {
        "Error: Missing required argument --config <PATH>\nUse --help for usage details."
            .to_string()
    })
}

fn print_help() {
    println!("Janus - Educational TCP and HTTP/1.1 Load Balancer & Reverse Proxy");
    println!();
    println!("Usage:");
    println!("  janus [OPTIONS]");
    println!();
    println!("Options:");
    println!("  -c, --config <PATH>    Path to the TOML configuration file (required)");
    println!("  -h, --help             Print help information");
}
