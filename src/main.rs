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
#[clap(override_usage = "zo-bots [OPTIONS]... <SUBCOMMAND>")]
struct Cli {
    #[clap(long, env = "ZO_STATE_PUBKEY")]
    zo_state_pubkey: Pubkey,
    #[clap(long, env = "SOLANA_CLUSTER_RPC_URL")]
    cluster_rpc_url: String,
    #[clap(long, env = "SOLANA_CLUSTER_WS_URL")]
    cluster_ws_url: String,
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

#[tokio::main]
async fn main() -> Result<(), lib::error::Error> {
    dotenv::dotenv().ok();

    let args = Cli::parse();

    let payer_key = env::var("SOLANA_PAYER_KEY")
        .ok()
        .and_then(|v| keypair::read_keypair(&mut v.as_bytes()).ok())
        .expect("Failed to parse SOLANA_PAYER_KEY");

    let cluster = Cluster::Custom(args.cluster_rpc_url, args.cluster_ws_url);
    let client = Client::new_with_options(
        cluster.clone(),
        payer_key,
        CommitmentConfig::confirmed(),
    );
    let program = client.program(zo_abi::ID);
    let rpc = program.rpc();
    let zo_state_pubkey = args.zo_state_pubkey;
    let zo_state: zo_abi::State = program.account(zo_state_pubkey).unwrap();
    let zo_cache: zo_abi::Cache = program.account(zo_state.cache).unwrap();

    let (zo_state_signer_pubkey, state_signer_nonce) =
        Pubkey::find_program_address(&[zo_state_pubkey.as_ref()], &zo_abi::ID);

    if state_signer_nonce != zo_state.signer_nonce {
        panic!("Invalid state signer nonce");
    }

    let (err_tx, err_rx) = tokio::sync::mpsc::channel(128);
    let (msg_tx, msg_rx) = tokio::sync::mpsc::channel(128);

    let app_state = lib::AppState {
        err_tx,
        cluster,
        client,
        program,
        rpc,
        zo_state,
        zo_cache,
        zo_state_pubkey: args.zo_state_pubkey,
        zo_cache_pubkey: zo_state.cache,
        zo_state_signer_pubkey,
    };

    let app_state: &'static _ = Box::leak(Box::new(app_state));

    let err_handle = tokio::spawn(lib::error::error_handler(err_rx));
    let msg_handle = tokio::spawn(lib::log::notify_worker(msg_rx));

    lib::log::init(msg_tx);

    match &args.command {
        Command::Crank {} => lib::crank::run(app_state).await?,
        Command::Listener {} => lib::listener::run(app_state).await?,
        Command::Consumer {
            to_consume,
            max_wait,
            max_queue_length,
        } => {
            lib::consumer::run(
                app_state,
                *to_consume,
                std::time::Duration::from_secs(*max_wait),
                *max_queue_length,
            )
            .await?
        }
        Command::Liquidator {
            num_workers,
            n,
        } => {
            lib::liquidator::run(
                app_state,
                zo_abi::dex::ID,
                zo_abi::ID,
                zo_abi::serum::ID,
                *num_workers,
                *n,
            )
            .await?
        }
    };

    let _ = err_handle.await;
    let _ = msg_handle.await;

    Ok(())
}
