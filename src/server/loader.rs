use std::{net::IpAddr, path::Path, sync::Arc};

use anyhow::{Context, Result};
use colored::Colorize;
use log::{error, info, warn};
use tokio::{fs, sync::RwLock, task::JoinSet};

use crate::{
    config::Config,
    devices::TapoDevice,
    discovery::DeviceCache,
};

pub async fn load_tapo_devices_from_config(
    config_path: &Path,
    cache: &DeviceCache,
    shared_cache: Arc<RwLock<DeviceCache>>,
    discovery_timeout_secs: u64,
) -> Result<(Config, Vec<TapoDevice>)> {
    let config_str = fs::read_to_string(&config_path)
        .await
        .context("Failed to read the devices configuration file")?;

    let mut config = serde_json::from_str::<Config>(&config_str)
        .context("Failed to parse the devices configuration file")?;

    // Resolve any missing `ip_addr` entries from the discovery cache by
    // matching the config-level `name` against the Tapo nickname.
    for conn_infos in config.devices.iter_mut() {
        if conn_infos.ip_addr.is_some() {
            continue;
        }
        match cache.lookup_by_nickname(&conn_infos.name) {
            Some(found) => match found.ip.parse::<IpAddr>() {
                Ok(addr) => {
                    conn_infos.ip_addr = Some(addr);
                    info!(
                        "Resolved {} -> {} (from cache)",
                        conn_infos.name.bright_yellow(),
                        addr
                    );
                }
                Err(err) => {
                    warn!(
                        "Cached IP '{}' for device '{}' is not parseable: {err}",
                        found.ip, conn_infos.name
                    );
                }
            },
            None => {
                error!(
                    "No cached IP for device '{}'; skipping. Add ip_addr to config or wait for discovery to find it (the device's nickname must match the config name).",
                    conn_infos.name
                );
            }
        }
    }

    let devices =
        load_tapo_devices(&config, shared_cache, discovery_timeout_secs).await?;

    Ok((config, devices))
}

async fn load_tapo_devices(
    config: &Config,
    shared_cache: Arc<RwLock<DeviceCache>>,
    discovery_timeout_secs: u64,
) -> Result<Vec<TapoDevice>> {
    let Config {
        devices,
        tapo_credentials,
        server_password: _,
        discovery_broadcast,
    } = config;

    let mut tasks = JoinSet::new();

    info!(
        "Attempting to connect to the {} configured device(s)...",
        devices.len()
    );

    let tapo_credentials = Arc::new(tapo_credentials.clone());

    for conn_infos in devices {
        // Skip entries that have neither a configured nor cached IP — the
        // resolution loop above will already have logged this.
        if conn_infos.ip_addr.is_none() {
            continue;
        }

        let tapo_credentials = Arc::clone(&tapo_credentials);
        let conn_infos = conn_infos.clone();
        let shared_cache = Arc::clone(&shared_cache);
        let broadcast = discovery_broadcast.clone();

        tasks.spawn(async move {
            let device = TapoDevice::new(
                conn_infos,
                tapo_credentials,
                shared_cache,
                broadcast,
                discovery_timeout_secs,
            );
            let conn_result = device.try_connect().await;
            (device, conn_result)
        });
    }

    let mut devices = vec![];

    while let Some(result) = tasks.join_next().await {
        let (device, conn_result) = result?;
        let name = &device.conn_infos().name;

        match conn_result {
            Ok(()) => info!("|> Device {} connected successfully!", name.bright_yellow()),

            Err(err) => error!("! Failed to connect to device '{name}': {err}",),
        }

        devices.push(device);
    }

    Ok(devices)
}
