use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Result;
use log::{info, warn};
use tokio::sync::RwLock;

use crate::{
    config::{Config, TapoCredentials},
    devices::TapoDevice,
    discovery::{self, DeviceCache},
};

use super::{loader::load_tapo_devices_from_config, sessions::Sessions};

pub struct StateData {
    pub config_path: PathBuf,
    pub loaded_config: RwLock<LoadedConfig>,
    pub sessions: Sessions,
    pub cache_path: PathBuf,
    pub device_cache: Arc<RwLock<DeviceCache>>,
    pub credentials: Arc<TapoCredentials>,
    pub discovery_broadcast: String,
    pub discovery_timeout_secs: u64,
}

impl StateData {
    pub async fn init(
        config_path: PathBuf,
        sessions_file: PathBuf,
        cache_path: PathBuf,
        discovery_interval: Duration,
        discovery_timeout_secs: u64,
    ) -> Result<Self> {
        // Load whatever is on disk now — main.rs already ran an initial
        // discovery sweep so this should be populated.
        let device_cache = Arc::new(RwLock::new(DeviceCache::load(&cache_path).await));

        let (config, devices) = {
            let cache_guard = device_cache.read().await;
            load_tapo_devices_from_config(
                &config_path,
                &cache_guard,
                Arc::clone(&device_cache),
                discovery_timeout_secs,
            )
            .await?
        };

        let credentials = Arc::new(config.tapo_credentials.clone());
        let discovery_broadcast = config.discovery_broadcast.clone();

        // Periodic background refresh task. Owns disk persistence; per-device
        // rediscovery in `devices.rs` only updates the in-memory cache.
        {
            let cache_clone = Arc::clone(&device_cache);
            let creds_clone = Arc::clone(&credentials);
            let broadcast_clone = discovery_broadcast.clone();
            let cache_path_clone = cache_path.clone();
            tokio::spawn(async move {
                if discovery_interval.is_zero() {
                    warn!("Tapo discovery interval is 0 — periodic refresh disabled.");
                    return;
                }
                let mut ticker = tokio::time::interval(discovery_interval);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                // Consume the immediate first tick so we don't double-discover
                // right after the initial sweep that main.rs ran.
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    info!(
                        "Periodic Tapo discovery tick — refreshing on {broadcast_clone}..."
                    );
                    let new_cache = discovery::discover_and_cache(
                        &creds_clone,
                        &broadcast_clone,
                        discovery_timeout_secs,
                        &cache_path_clone,
                    )
                    .await;
                    *cache_clone.write().await = new_cache;
                }
            });
        }

        Ok(Self {
            config_path,
            loaded_config: RwLock::new(LoadedConfig::new(config, devices)),
            sessions: Sessions::create(sessions_file).await?,
            cache_path,
            device_cache,
            credentials,
            discovery_broadcast,
            discovery_timeout_secs,
        })
    }

    pub async fn reload_config(&self) -> Result<()> {
        let cache_guard = self.device_cache.read().await;
        let (config, devices) = load_tapo_devices_from_config(
            &self.config_path,
            &cache_guard,
            Arc::clone(&self.device_cache),
            self.discovery_timeout_secs,
        )
        .await?;
        drop(cache_guard);

        *self.loaded_config.write().await = LoadedConfig::new(config, devices);

        Ok(())
    }
}

pub struct LoadedConfig {
    pub config: Config,
    pub devices: HashMap<String, TapoDevice>,
}

impl LoadedConfig {
    pub fn new(config: Config, devices: Vec<TapoDevice>) -> Self {
        Self {
            config,
            devices: devices
                .into_iter()
                .map(|device| (device.conn_infos().name.to_owned(), device))
                .collect(),
        }
    }
}
