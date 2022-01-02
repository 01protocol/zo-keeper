use anchor_client::{
    solana_sdk::{
        commitment_config::CommitmentConfig, pubkey::Pubkey, signer::keypair,
    },
    Client, Cluster,
};
use clap::{Parser, Subcommand};
use std::env;

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
        #[clap(long, default_value = "32")]
        to_consume: usize,
        #[clap(long, default_value = "60", help = "seconds")]
        max_wait: u64,
        #[clap(long, default_value = "1")]
        max_queue_length: usize,
    },
    Liquidator {},
}

#[tokio::main]
async fn main() -> Result<(), lib::error::Error> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt::init();

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

    let (tx, rx) = tokio::sync::mpsc::channel(128);

    let app_state = lib::AppState {
        err_tx: tx,
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

    let err_handle = tokio::spawn(lib::error::error_handler(rx));

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
        Command::Liquidator {} => lib::liquidator::run(app_state).await?,
    };

    let _ = err_handle.await;

    Ok(())
}
