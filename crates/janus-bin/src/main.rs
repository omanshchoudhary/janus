#![deny(clippy::all)]
#![warn(clippy::pedantic)]

use std::env;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = env::args().collect();

    match parse_args(&args) {
        Ok(config_path) => {
            tracing::info!("Janus starting with config: {}", config_path);
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
