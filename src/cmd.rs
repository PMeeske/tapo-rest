use std::path::PathBuf;

use clap::Parser;
use log::LevelFilter;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
pub struct Cmd {
    #[clap(help = "Path to the configuration file (.json)")]
    pub config_path: PathBuf,

    #[clap(short, long, env, help = "Port to serve on")]
    pub port: u16,

    #[clap(
        short,
        long,
        global = true,
        help = "Level of verbosity",
        default_value = "info"
    )]
    pub verbosity: LevelFilter,

    #[clap(
        long,
        env = "TAPO_DISCOVERY_INTERVAL_SECS",
        default_value = "300",
        help = "Interval (seconds) between background Tapo LAN discovery refreshes"
    )]
    pub discovery_interval_secs: u64,

    #[clap(
        long,
        env = "TAPO_DISCOVERY_TIMEOUT_SECS",
        default_value = "10",
        help = "Per-discovery wait time (seconds, 1..=60)"
    )]
    pub discovery_timeout_secs: u64,
}
