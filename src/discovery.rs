//! Tapo LAN auto-discovery.
//!
//! Wraps the `tapo` crate's `ApiClient::discover_devices` so the gateway can
//! resolve device IPs at startup and refresh them periodically without
//! requiring `ip_addr` entries in `config.json`.
//!
//! Persistence layout: `<state_dir>/tapo-rest/tapo_devices.json` — a single
//! JSON document containing a `devices: Vec<DiscoveredDevice>` array. Atomic
//! writes go through a sibling `.json.tmp` file plus `tokio::fs::rename`.
//!
//! Concurrency: the on-disk cache is loaded once at boot. After that the
//! in-memory `DeviceCache` lives behind an `Arc<RwLock<...>>` shared with the
//! periodic refresh task and per-device rediscovery (see `server::state`,
//! `devices::TapoDevice`). Disk persistence is owned by the periodic task —
//! on-demand rediscovery only mutates the in-memory cache.

use std::path::Path;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use tapo::ApiClient;
use tokio::fs;
use tokio::sync::RwLock;

use crate::config::TapoCredentials;

/// One row in the persisted Tapo discovery cache.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DiscoveredDevice {
    pub device_id: String,
    pub model: String,
    pub nickname: String,
    pub ip: String,
    pub last_seen: DateTime<Utc>,
}

/// Persisted snapshot of the LAN, indexed by `device_id` for merge semantics.
#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct DeviceCache {
    pub devices: Vec<DiscoveredDevice>,
}

impl DeviceCache {
    /// Load the cache from disk. A missing or unparseable file produces a
    /// fresh empty cache and a `warn!` log entry rather than an error: a
    /// corrupted cache should never block server startup.
    pub async fn load(path: &Path) -> Self {
        match fs::read_to_string(path).await {
            Ok(contents) => match serde_json::from_str::<DeviceCache>(&contents) {
                Ok(cache) => {
                    info!(
                        "Loaded Tapo device cache from {} ({} entries)",
                        path.display(),
                        cache.devices.len()
                    );
                    cache
                }
                Err(err) => {
                    warn!(
                        "Failed to parse Tapo device cache at {}: {err} — starting with an empty cache",
                        path.display()
                    );
                    DeviceCache::default()
                }
            },
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => DeviceCache::default(),
            Err(err) => {
                warn!(
                    "Failed to read Tapo device cache at {}: {err} — starting with an empty cache",
                    path.display()
                );
                DeviceCache::default()
            }
        }
    }

    /// Atomically persist the cache: serialize, write to `<path>.tmp`, then
    /// rename. The rename is atomic on POSIX and Windows for same-filesystem
    /// targets, which matches our use of a sibling temp file.
    pub async fn save(&self, path: &Path) -> Result<()> {
        let serialized = serde_json::to_vec_pretty(self)
            .context("Failed to serialize Tapo device cache")?;

        let tmp_path = path.with_extension("json.tmp");
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                fs::create_dir_all(parent).await.with_context(|| {
                    format!(
                        "Failed to create parent directory for Tapo device cache: {}",
                        parent.display()
                    )
                })?;
            }
        }

        fs::write(&tmp_path, &serialized)
            .await
            .with_context(|| format!("Failed to write Tapo device cache temp file at {}", tmp_path.display()))?;

        fs::rename(&tmp_path, path).await.with_context(|| {
            format!(
                "Failed to rename Tapo device cache temp file {} -> {}",
                tmp_path.display(),
                path.display()
            )
        })?;

        Ok(())
    }

    pub fn lookup_by_nickname(&self, nickname: &str) -> Option<&DiscoveredDevice> {
        self.devices.iter().find(|d| d.nickname == nickname)
    }

    pub fn lookup_by_device_id(&self, id: &str) -> Option<&DiscoveredDevice> {
        self.devices.iter().find(|d| d.device_id == id)
    }

    /// Merge a fresh discovery batch into this cache.
    ///
    /// Replace-by-`device_id` semantics: any device seen this round overwrites
    /// the previous entry, but devices that briefly drop off the LAN are kept
    /// so a single missed broadcast does not orphan them.
    fn merge_in(&mut self, fresh: Vec<DiscoveredDevice>) {
        for new_dev in fresh {
            if let Some(existing) = self
                .devices
                .iter_mut()
                .find(|d| d.device_id == new_dev.device_id)
            {
                *existing = new_dev;
            } else {
                self.devices.push(new_dev);
            }
        }
    }
}

/// Run a single Tapo LAN discovery sweep.
///
/// Builds a fresh `ApiClient` (since `discover_devices` consumes by value),
/// drives the resulting `Stream` to completion, and accumulates each
/// `Ok(DiscoveryResult)` into a `DiscoveredDevice`. Per-host failures
/// (unreachable, auth error, etc.) are logged at `warn!` and skipped — they
/// do not abort the sweep.
pub async fn discover(
    credentials: &TapoCredentials,
    broadcast: &str,
    timeout_s: u64,
) -> Result<Vec<DiscoveredDevice>> {
    let client = ApiClient::new(&credentials.email, &credentials.password);

    info!(
        "Starting Tapo discovery on {broadcast} (timeout {timeout_s}s)..."
    );

    let mut stream = client
        .discover_devices(broadcast.to_string(), timeout_s)
        .await
        .with_context(|| format!("Tapo discover_devices({broadcast}) failed"))?;

    let mut found = Vec::new();

    while let Some(item) = stream.next().await {
        match item {
            Ok(result) => {
                let device = DiscoveredDevice {
                    device_id: result.device_id().to_string(),
                    model: result.model().to_string(),
                    nickname: result.nickname().to_string(),
                    ip: result.ip().to_string(),
                    last_seen: Utc::now(),
                };
                info!(
                    "Discovered Tapo device '{}' ({}) at {} [id={}]",
                    device.nickname, device.model, device.ip, device.device_id
                );
                found.push(device);
            }
            Err(err) => {
                warn!("Tapo discovery yielded a per-host error: {err}");
            }
        }
    }

    info!(
        "Tapo discovery: found {} device(s) on {broadcast}",
        found.len()
    );

    Ok(found)
}

/// Run a discovery sweep and persist the merged cache to disk.
///
/// On success: load the existing cache, merge in fresh results (replace by
/// `device_id`), write to disk, return the merged cache. On error: log
/// `error!` and return the previously-loaded cache unchanged so the server
/// can keep using whatever IPs it knew about.
pub async fn discover_and_cache(
    credentials: &TapoCredentials,
    broadcast: &str,
    timeout_s: u64,
    cache_path: &Path,
) -> DeviceCache {
    let mut cache = DeviceCache::load(cache_path).await;

    match discover(credentials, broadcast, timeout_s).await {
        Ok(fresh) => {
            cache.merge_in(fresh);
            if let Err(err) = cache.save(cache_path).await {
                warn!(
                    "Failed to persist Tapo device cache to {}: {err}",
                    cache_path.display()
                );
            }
        }
        Err(err) => {
            error!(
                "Tapo discovery failed on {broadcast}: {err} — keeping previously-loaded cache ({} entries)",
                cache.devices.len()
            );
        }
    }

    cache
}

/// In-memory only refresh helper used by `TapoDevice::_establish_conn` when a
/// connection fails and we need to re-resolve a single device. Disk
/// persistence is intentionally skipped here — the periodic background task
/// in `server::state` owns the cache file.
pub async fn refresh_in_memory(
    cache: &RwLock<DeviceCache>,
    credentials: &TapoCredentials,
    broadcast: &str,
    timeout_s: u64,
) {
    match discover(credentials, broadcast, timeout_s).await {
        Ok(fresh) => {
            let mut guard = cache.write().await;
            guard.merge_in(fresh);
        }
        Err(err) => {
            warn!(
                "On-demand Tapo rediscovery on {broadcast} failed: {err} — keeping in-memory cache as-is"
            );
        }
    }
}
