#![forbid(unsafe_code)]
#![forbid(unused_must_use)]
#![warn(unused_crate_dependencies)]
// Use logging instead
#![deny(clippy::print_stdout)]
#![deny(clippy::print_stderr)]

use std::process::ExitCode;

use anyhow::{Context, Result, bail};
use clap::Parser;
use log::{error, info};
use tokio::fs;

use crate::cmd::Cmd;

use self::logger::Logger;

mod cmd;
mod config;
mod devices;
mod discovery;
mod logger;
mod server;

#[tokio::main]
async fn main() -> ExitCode {
    match inner_main().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            error!("{err:?}");
            ExitCode::FAILURE
        }
    }
}

async fn inner_main() -> Result<()> {
    let Cmd {
        config_path,
        port,
        verbosity,
        discovery_interval_secs,
        discovery_timeout_secs,
    } = Cmd::parse();

    // Set up the logger
    Logger::new(verbosity).init().unwrap();

    let data_dir = dirs::state_dir()
        .or_else(dirs::data_local_dir)
        .context("Failed to find a valid local data directory")?
        .join(env!("CARGO_PKG_NAME"));

    if !data_dir.exists() {
        fs::create_dir_all(&data_dir)
            .await
            .context("Failed to create a local data directory")?;
    }

    if !config_path.is_file() {
        bail!(
            "Configuration was not found at path {}",
            config_path.to_string_lossy()
        );
    }

    let cache_path = data_dir.join("tapo_devices.json");

    // Pre-read the config so we can run an initial Tapo LAN discovery before
    // `server::serve` boots the device loader (which depends on the cache).
    // Loader.rs still owns the canonical parse — this is just a peek.
    let config_str = fs::read_to_string(&config_path)
        .await
        .context("Failed to read the devices configuration file for initial discovery")?;
    let initial_config = serde_json::from_str::<config::Config>(&config_str)
        .context("Failed to parse the devices configuration file for initial discovery")?;
    let initial_credentials = initial_config.tapo_credentials.clone();
    let initial_broadcast = initial_config.discovery_broadcast.clone();
    drop(initial_config);

    info!(
        "Running initial Tapo LAN discovery on {initial_broadcast} (cache at {})...",
        cache_path.display()
    );
    let initial_cache = discovery::discover_and_cache(
        &initial_credentials,
        &initial_broadcast,
        discovery_timeout_secs,
        &cache_path,
    )
    .await;
    info!(
        "Initial Tapo discovery complete: {} cached device(s)",
        initial_cache.devices.len()
    );

    info!("Now launching server...");

    server::serve(
        port,
        config_path,
        data_dir.join("sessions.json"),
        cache_path,
        std::time::Duration::from_secs(discovery_interval_secs),
        discovery_timeout_secs,
    )
    .await
}
