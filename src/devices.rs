use std::{net::IpAddr, sync::Arc};

use anyhow::{Result, anyhow};
use log::{debug, info, warn};
use tapo::{
    ApiClient, ColorLightHandler, LightHandler, PlugEnergyMonitoringHandler, PlugHandler,
    PowerStripEnergyMonitoringHandler, PowerStripHandler, RgbLightStripHandler,
    RgbicLightStripHandler,
};
use tokio::sync::RwLock;

use crate::{
    config::{TapoConnectionInfos, TapoCredentials},
    discovery::{self, DeviceCache},
    server::TapoDeviceType,
};

pub struct TapoDevice {
    /// Original config snapshot — `conn_infos.ip_addr` is the user-supplied
    /// "last-known good" IP (may be `None` if the user omitted it). The
    /// authoritative current IP lives in `current_ip` so we can mutate it
    /// after rediscovery without touching the config.
    conn_infos: TapoConnectionInfos,
    credentials: Arc<TapoCredentials>,
    client: RwLock<Option<TapoDeviceInner>>,
    /// IP currently in use for connection attempts. Initialized from
    /// `conn_infos.ip_addr` (resolved by loader from the cache) and updated
    /// on rediscovery.
    current_ip: RwLock<Option<IpAddr>>,
    cache: Arc<RwLock<DeviceCache>>,
    discovery_broadcast: String,
    discovery_timeout_secs: u64,
}

impl TapoDevice {
    pub fn new(
        conn_infos: TapoConnectionInfos,
        credentials: Arc<TapoCredentials>,
        cache: Arc<RwLock<DeviceCache>>,
        discovery_broadcast: String,
        discovery_timeout_secs: u64,
    ) -> Self {
        let initial_ip = conn_infos.ip_addr;
        Self {
            conn_infos,
            credentials,
            client: RwLock::new(None),
            current_ip: RwLock::new(initial_ip),
            cache,
            discovery_broadcast,
            discovery_timeout_secs,
        }
    }

    pub fn conn_infos(&self) -> &TapoConnectionInfos {
        &self.conn_infos
    }

    // pub async fn is_connected(&self) -> bool {
    //     self.client.read().await.is_some()
    // }

    pub async fn try_connect(&self) -> Result<()> {
        self.with_client(async |_| {}).await
    }

    pub async fn with_client<T>(&self, func: impl AsyncFnOnce(&TapoDeviceInner) -> T) -> Result<T> {
        {
            if let Some(conn) = &*self.client.read().await {
                return Ok(func(conn).await);
            }
        }

        self.with_client_mut(async move |client| func(&*client).await)
            .await
    }

    pub async fn with_client_mut<T>(
        &self,
        func: impl AsyncFnOnce(&mut TapoDeviceInner) -> T,
    ) -> Result<T> {
        let mut conn_lock = self.client.write().await;

        if let Some(conn) = conn_lock.as_mut() {
            return Ok(func(conn).await);
        }

        let mut conn = self._establish_conn().await?;

        debug!(
            "Established a connection with device '{}'!",
            self.conn_infos.name
        );

        let out = func(&mut conn).await;
        *conn_lock = Some(conn);
        Ok(out)
    }

    pub async fn refresh_session(&self) -> Result<()> {
        self.with_client_mut(async |conn| -> Result<()> {
            // Call '.refresh_session()' on the device client
            macro_rules! refresh_session {
                ($conn: expr => $($enum_variant: ident),+) => {{
                    match $conn {
                        $(TapoDeviceInner::$enum_variant(device) => {
                            device.refresh_session().await?;
                        })+
                    }
                }}
            }

            refresh_session!(conn =>
                    L510, L520, L530, L535,
                    L610, L630,
                    L900, L920, L930,
                    P100, P105, P110, P110M, P115,
                    P300, P304, P304M, P316
            );

            Ok(())
        })
        .await?
    }

    async fn _establish_conn(&self) -> Result<TapoDeviceInner> {
        let name = &self.conn_infos.name;
        let device_type = &self.conn_infos.device_type;

        let initial_ip = *self.current_ip.read().await;

        // First attempt — use whatever IP we currently have (config-supplied
        // or cache-resolved). If `None`, jump straight to rediscovery.
        if let Some(ip) = initial_ip {
            match self.connect_at(ip).await {
                Ok(conn) => return Ok(conn),
                Err(err) => {
                    warn!(
                        "Connection to '{name}' at {ip} failed ({err}) — attempting rediscovery..."
                    );
                }
            }
        } else {
            info!(
                "Device '{name}' has no known IP yet — running rediscovery..."
            );
        }

        // Rediscover. Updates the in-memory cache only; disk persistence is
        // owned by the periodic task in `server::state`.
        discovery::refresh_in_memory(
            &self.cache,
            &self.credentials,
            &self.discovery_broadcast,
            self.discovery_timeout_secs,
        )
        .await;

        let resolved_ip: Option<IpAddr> = {
            let guard = self.cache.read().await;
            guard
                .lookup_by_nickname(name)
                .and_then(|d| d.ip.parse::<IpAddr>().ok())
        };

        if let Some(new_ip) = resolved_ip {
            if Some(new_ip) != initial_ip {
                info!("Rediscovered '{name}' -> {new_ip}");
                *self.current_ip.write().await = Some(new_ip);
            }
            match self.connect_at(new_ip).await {
                Ok(conn) => return Ok(conn),
                Err(err) => {
                    warn!(
                        "Retry connection to '{name}' at {new_ip} failed: {err}"
                    );
                }
            }
        }

        // Final fallback: last-known IP if we have one and didn't already use
        // it for the first attempt.
        if let Some(fallback_ip) = initial_ip {
            if resolved_ip != Some(fallback_ip) {
                info!(
                    "Falling back to last-known IP {fallback_ip} for '{name}'..."
                );
                if let Ok(conn) = self.connect_at(fallback_ip).await {
                    return Ok(conn);
                }
            }
        }

        Err(anyhow!(
            "Failed to connect to {} {} '{name}': discovery yielded no entry for '{name}' and no fallback available",
            device_type.type_name(),
            device_type.type_description()
        ))
    }

    async fn connect_at(&self, ip: IpAddr) -> Result<TapoDeviceInner> {
        let TapoCredentials { email, password } = &*self.credentials;
        let tapo_client = ApiClient::new(email, password);
        let ip_str = ip.to_string();
        let device_type = &self.conn_infos.device_type;
        let name = &self.conn_infos.name;

        let conn = match device_type {
            TapoDeviceType::L510 => tapo_client.l510(ip_str).await.map(TapoDeviceInner::L510),
            TapoDeviceType::L520 => tapo_client.l520(ip_str).await.map(TapoDeviceInner::L520),
            TapoDeviceType::L530 => tapo_client.l530(ip_str).await.map(TapoDeviceInner::L530),
            TapoDeviceType::L535 => tapo_client.l535(ip_str).await.map(TapoDeviceInner::L535),
            TapoDeviceType::L610 => tapo_client.l610(ip_str).await.map(TapoDeviceInner::L610),
            TapoDeviceType::L630 => tapo_client.l630(ip_str).await.map(TapoDeviceInner::L630),
            TapoDeviceType::L900 => tapo_client.l900(ip_str).await.map(TapoDeviceInner::L900),
            TapoDeviceType::L920 => tapo_client.l920(ip_str).await.map(TapoDeviceInner::L920),
            TapoDeviceType::L930 => tapo_client.l930(ip_str).await.map(TapoDeviceInner::L930),
            TapoDeviceType::P100 => tapo_client.p100(ip_str).await.map(TapoDeviceInner::P100),
            TapoDeviceType::P105 => tapo_client.p105(ip_str).await.map(TapoDeviceInner::P105),
            TapoDeviceType::P110 => tapo_client.p110(ip_str).await.map(TapoDeviceInner::P110),
            TapoDeviceType::P110M => tapo_client.p110(ip_str).await.map(TapoDeviceInner::P110M),
            TapoDeviceType::P115 => tapo_client.p115(ip_str).await.map(TapoDeviceInner::P115),
            TapoDeviceType::P300 => tapo_client.p300(ip_str).await.map(TapoDeviceInner::P300),
            TapoDeviceType::P304 => tapo_client.p304(ip_str).await.map(TapoDeviceInner::P304),
            TapoDeviceType::P304M => tapo_client.p304(ip_str).await.map(TapoDeviceInner::P304M),
            TapoDeviceType::P316 => tapo_client.p316(ip_str).await.map(TapoDeviceInner::P316),
        };

        conn.map_err(|err| {
            anyhow!(
                "Failed to connect to {} {} '{name}' at {ip}: {err}",
                device_type.type_name(),
                device_type.type_description()
            )
        })
    }
}

pub enum TapoDeviceInner {
    L510(LightHandler),
    L520(LightHandler),
    L530(ColorLightHandler),
    L535(ColorLightHandler),
    L610(LightHandler),
    L630(ColorLightHandler),
    L900(RgbLightStripHandler),
    L920(RgbicLightStripHandler),
    L930(RgbicLightStripHandler),
    P100(PlugHandler),
    P105(PlugHandler),
    P110(PlugEnergyMonitoringHandler),
    P110M(PlugEnergyMonitoringHandler),
    P115(PlugEnergyMonitoringHandler),
    P300(PowerStripHandler),
    P304(PowerStripEnergyMonitoringHandler),
    P304M(PowerStripEnergyMonitoringHandler),
    P316(PowerStripEnergyMonitoringHandler),
}

impl TapoDeviceInner {
    pub fn type_name(&self) -> &'static str {
        match self {
            TapoDeviceInner::L510(_) => "L510",
            TapoDeviceInner::L520(_) => "L520",
            TapoDeviceInner::L530(_) => "L530",
            TapoDeviceInner::L535(_) => "L535",
            TapoDeviceInner::L610(_) => "L610",
            TapoDeviceInner::L630(_) => "L630",
            TapoDeviceInner::L900(_) => "L900",
            TapoDeviceInner::L920(_) => "L920",
            TapoDeviceInner::L930(_) => "L930",
            TapoDeviceInner::P100(_) => "P100",
            TapoDeviceInner::P105(_) => "P105",
            TapoDeviceInner::P110(_) => "P110",
            TapoDeviceInner::P110M(_) => "P110M",
            TapoDeviceInner::P115(_) => "P115",
            TapoDeviceInner::P300(_) => "P300",
            TapoDeviceInner::P304(_) => "P304",
            TapoDeviceInner::P304M(_) => "P304M",
            TapoDeviceInner::P316(_) => "P316",
        }
    }
}
