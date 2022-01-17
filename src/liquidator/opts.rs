use anchor_client::Cluster;

use clap::Parser;

use solana_sdk::pubkey::Pubkey;

/// Program to find and liquidate accounts on the 01 dex
#[derive(Parser, Debug, Clone)]
#[clap(name = "liquidator")]
pub struct Opts {
    /// The cluster to be used
    #[clap(long, default_value = "mainnet")]
    pub cluster: Cluster,

    /// The address of the endpoint to use for querying the chain
    #[clap(short, long, default_value = "http://127.0.0.1:8899")]
    pub http_endpoint: String,

    #[clap(short, long, default_value = "ws://127.0.0.1:8900")]
    pub ws_endpoint: String,

    /// Address of the dex for which to find liquidatable accounts
    #[clap(short, long)]
    pub dex_program: Pubkey,

    /// Address of the 01 program
    #[clap(short, long)]
    pub zo_program: Pubkey,

    #[clap(short, long)]
    pub serum_dex_program: Pubkey,

    /// The number of bots that are running
    #[clap(long)]
    pub num_workers: u8,

    /// The thread this bot is responsible for
    #[clap(long)]
    pub n: u8,
}

impl Opts {
    pub fn get_args() -> Opts {
        Opts::parse()
    }
}
