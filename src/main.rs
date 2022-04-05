use anchor_client::{
    solana_sdk::{commitment_config::CommitmentConfig, signer::keypair},
    Cluster,
};
use clap::{Parser, Subcommand};
use std::{env, time::Duration};
use zo_keeper as lib;

#[derive(Parser)]
#[clap(term_width = 72, disable_help_subcommand = true)]
struct Cli {
    /// RPC endpoint.
    #[clap(short, long, env = "SOLANA_RPC_URL")]
    rpc_url: String,

    /// Websocket endpoint.
    #[clap(long, env = "SOLANA_WS_URL")]
    ws_url: String,

    /// Path to keypair. If not set, the JSON encoded keypair is read
    /// from $SOLANA_PAYER_KEY instead.
    #[clap(short, long)]
    payer: Option<std::path::PathBuf>,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run caching and update funding instructions
    Crank {
        /// Interval for cache oracle, in seconds
        #[clap(long, default_value = "2.5", parse(try_from_str = parse_seconds))]
        cache_oracle_interval: Duration,

        /// Interval for cache interest, in seconds
        #[clap(long, default_value = "5", parse(try_from_str = parse_seconds))]
        cache_interest_interval: Duration,

        /// Interval for update funding, in seconds
        #[clap(long, default_value = "15", parse(try_from_str = parse_seconds))]
        update_funding_interval: Duration,
    },

    /// Consume events for each market
    Consumer {
        /// Events to consume each iteration
        #[clap(long, default_value = "12")]
        to_consume: usize,

        /// Maximum time to stay idle, in seconds
        #[clap(long, default_value = "30", parse(try_from_str = parse_seconds))]
        max_wait: Duration,

        /// Maximum queue length before processing
        #[clap(long, default_value = "1")]
        max_queue_length: usize,
    },

    /// Find liquidatable accounts and liquidate them
    Liquidator {
        /// The total number of bots run
        #[clap(long, default_value = "1")]
        worker_count: u8,

        /// The slice of addresses this bot is responsible for
        #[clap(long, default_value = "0")]
        worker_index: u8,
    },

    /// Listen and store events into a database
    Recorder,
}

fn main() -> Result<(), lib::Error> {
    dotenv::dotenv().ok();

    {
        use tracing_subscriber::{util::SubscriberInitExt, EnvFilter};

        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            // https://no-color.org/
            .with_ansi(env::var_os("NO_COLOR").is_none())
            .finish()
            .init();
    }

    let Cli {
        rpc_url,
        ws_url,
        payer,
        command,
    } = Cli::parse();

    let payer = match payer {
        Some(p) => keypair::read_keypair_file(&p).unwrap_or_else(|_| {
            panic!("Failed to read keypair from {}", p.to_string_lossy())
        }),
        None => match env::var("SOLANA_PAYER_KEY").ok() {
            Some(k) => keypair::read_keypair(&mut k.as_bytes())
                .expect("Failed to parse $SOLANA_PAYER_KEY"),
            None => panic!("Could not load payer key,"),
        },
    };

    let cluster = Cluster::Custom(rpc_url, ws_url);
    let commitment = match command {
        Command::Crank { .. } => CommitmentConfig::processed(),
        _ => CommitmentConfig::confirmed(),
    };

    let app_state: &'static _ =
        Box::leak(Box::new(lib::AppState::new(cluster, commitment, payer)));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    match command {
        Command::Liquidator {
            worker_count,
            worker_index,
        } => {
            rt.block_on(lib::liquidator::run(
                app_state,
                worker_count,
                worker_index,
            ))?;
        }
        Command::Crank {
            cache_oracle_interval,
            cache_interest_interval,
            update_funding_interval,
        } => rt.block_on(lib::crank::run(
            app_state,
            lib::crank::CrankConfig {
                cache_oracle_interval,
                cache_interest_interval,
                update_funding_interval,
            },
        ))?,
        Command::Consumer {
            to_consume,
            max_wait,
            max_queue_length,
        } => rt.block_on(lib::consumer::run(
            app_state,
            lib::consumer::ConsumerConfig {
                to_consume,
                max_wait,
                max_queue_length,
            },
        ))?,
        Command::Recorder => rt.block_on(lib::recorder::run(app_state))?,
    };

    Ok(())
}

fn parse_seconds(s: &str) -> Result<Duration, std::num::ParseFloatError> {
    <f64 as std::str::FromStr>::from_str(s).map(Duration::from_secs_f64)
}
