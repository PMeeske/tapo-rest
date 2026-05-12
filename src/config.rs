use std::{env, net::IpAddr};

use axum::http::HeaderValue;
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

/// Environment variable controlling the CORS origin allowlist.
///
/// Format: comma-separated list of fully-qualified origin URLs, e.g.
/// `"https://operator.example.com,http://127.0.0.1:8080"`.
///
/// SEC-01 / E-4: replaces the previous wildcard `AllowOrigin::any()`.
pub const TAPO_CORS_ORIGINS_ENV: &str = "TAPO_CORS_ORIGINS";

/// Parse the CORS origin allowlist from `TAPO_CORS_ORIGINS`.
///
/// Returns the comma-separated entries when the env var is set (entries
/// that fail to parse as `HeaderValue` are skipped). Falls back to the
/// Tailscale-loopback defaults (`http://127.0.0.1` and `http://localhost`)
/// when the variable is unset or contains no parsable entries.
///
/// Per SEC-01 / E-4 the operator opts in to broader origins explicitly —
/// the binary never reverts to `AllowOrigin::any()` from this surface.
pub fn cors_origins() -> Vec<HeaderValue> {
    let raw = env::var(TAPO_CORS_ORIGINS_ENV).unwrap_or_default();

    let parsed: Vec<HeaderValue> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(|s| HeaderValue::from_str(s).ok())
        .collect();

    if !parsed.is_empty() {
        return parsed;
    }

    vec![
        HeaderValue::from_static("http://127.0.0.1"),
        HeaderValue::from_static("http://localhost"),
    ]
}

