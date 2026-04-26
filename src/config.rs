use std::net::IpAddr;

use serde::{Deserialize, Serialize};

use crate::server::TapoDeviceType;

#[derive(Serialize, Deserialize)]
pub struct Config {
    pub tapo_credentials: TapoCredentials,
    pub devices: Vec<TapoConnectionInfos>,
    pub server_password: String,
    /// Broadcast (or unicast) target used for periodic Tapo LAN discovery.
    /// Operators can override this per-LAN, e.g. `"192.168.1.255"`.
    /// Defaults to `"255.255.255.255"` (global broadcast) when omitted.
    #[serde(default = "default_broadcast")]
    pub discovery_broadcast: String,
}

fn default_broadcast() -> String {
    "255.255.255.255".to_string()
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TapoCredentials {
    pub email: String,
    pub password: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TapoConnectionInfos {
    pub name: String,
    pub device_type: TapoDeviceType,
    /// Optional explicit IP. When omitted, the server resolves the device's
    /// IP via Tapo LAN discovery and matches by `name` (which must equal
    /// the device's nickname in the Tapo app).
    #[serde(default)]
    pub ip_addr: Option<IpAddr>,
}
