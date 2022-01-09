use anchor_client::{
    solana_sdk::{
        commitment_config::CommitmentConfig, pubkey::Pubkey, signer::keypair,
    },
    Client, Cluster,
};
use clap::{Parser, Subcommand};
use std::env;
use zo_keeper as lib;

#[derive(Parser)]
#[clap(term_width = 72)]
struct Cli {
    /// Name of cluster or its RPC endpoint.
    #[clap(short, long, env = "SOLANA_CLUSTER", default_value = "devnet")]
    cluster: Cluster,

    /// Path to keypair. If not set, the JSON encoded keypair is read
    /// from $SOLANA_PAYER_KEY instead.
    #[clap(short, long)]
    payer: Option<std::path::PathBuf>,

    /// Pubkey for the zo state struct.
    #[clap(long, env = "ZO_STATE_PUBKEY")]
    zo_state_pubkey: Pubkey,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Crank {},
    Listener {},
    Consumer {
        /// Events to consume each iteration
        #[clap(long, default_value = "32")]
        to_consume: usize,

        /// Maximum time to stay idle, in seconds
        #[clap(long, default_value = "60")]
        max_wait: u64,

        /// Maximum queue length before processing
        #[clap(long, default_value = "1")]
        max_queue_length: usize,
    },
    Liquidator {
        /// The number of bots that are running
        #[clap(long, default_value = "1")]
        num_workers: u8,

        /// The thread this bot is responsible for
        #[clap(long, default_value = "0")]
        n: u8,
    },
}

fn main() -> Result<(), lib::error::Error> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt::init();

    let Cli {
        cluster,
        payer,
        zo_state_pubkey,
        command,
    } = Cli::parse();

    let payer = match payer {
        Some(p) => keypair::read_keypair_file(&p).unwrap_or_else(|_| {
            panic!("Failed to read keypair from {}", p.to_string_lossy())
        }),
        None => match env::var("SOLANA_PAYER_KEY").ok() {
            Some(k) => keypair::read_keypair(&mut k.as_bytes())
                .ok()
                .expect("Failed to parse $SOLANA_PAYER_KEY"),
            None => panic!("Could not load payer key,"),
        },
    };

    let client = Client::new_with_options(
        cluster.clone(),
        payer,
        CommitmentConfig::confirmed(),
    );

    let program = client.program(zo_abi::ID);
    let rpc = program.rpc();
    let zo_state: zo_abi::State = program.account(zo_state_pubkey).unwrap();
    let zo_cache: zo_abi::Cache = program.account(zo_state.cache).unwrap();

    let (zo_state_signer_pubkey, state_signer_nonce) =
        Pubkey::find_program_address(&[zo_state_pubkey.as_ref()], &zo_abi::ID);

    if state_signer_nonce != zo_state.signer_nonce {
        panic!("Invalid state signer nonce");
    }

    let app_state = lib::AppState {
        cluster,
        client,
        program,
        rpc,
        zo_state,
        zo_cache,
        zo_state_pubkey,
        zo_cache_pubkey: zo_state.cache,
        zo_state_signer_pubkey,
    };

    let app_state: &'static _ = Box::leak(Box::new(app_state));

    match command {
        Command::Liquidator { num_workers, n } => {
            lib::liquidator::run(
                app_state,
                zo_abi::dex::ID,
                zo_abi::ID,
                zo_abi::serum::ID,
                num_workers,
                n,
            )?;
        }
        c => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();

            match c {
                Command::Crank {} => rt.block_on(lib::crank::run(app_state))?,
                Command::Listener {} => {
                    rt.block_on(lib::listener::run(app_state))?
                }
                Command::Consumer {
                    to_consume,
                    max_wait,
                    max_queue_length,
                } => rt.block_on(lib::consumer::run(
                    app_state,
                    to_consume,
                    std::time::Duration::from_secs(max_wait),
                    max_queue_length,
                ))?,
                _ => panic!(),
            }
        }
    };

    Ok(())
}
