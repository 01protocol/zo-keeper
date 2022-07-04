use anchor_client::{
    solana_client::{
        nonblocking::pubsub_client::PubsubClient,
        rpc_config::{RpcTransactionLogsConfig, RpcTransactionLogsFilter},
        rpc_response::RpcLogsResponse,
    },
    solana_sdk::commitment_config::CommitmentConfig,
};
use futures::StreamExt as _;
use zo_abi as zo;

pub async fn run(st: &'static crate::AppState) -> Result<(), crate::Error> {
    let cli = PubsubClient::new(st.cluster.ws_url()).await.unwrap();
    let (mut rx, _unsub) = cli
        .logs_subscribe(
            RpcTransactionLogsFilter::Mentions(vec![zo::ID.to_string()]),
            RpcTransactionLogsConfig {
                commitment: Some(CommitmentConfig::finalized()),
            },
        )
        .await
        .unwrap();

    while let Some(x) = rx.next().await {
        tokio::task::spawn_blocking(move || print_log(&x.value));
    }

    Ok(())
}

fn print_log(r: &RpcLogsResponse) {
    let status = if r.err.is_none() { "ok " } else { "err" };

    let mut i = 0;
    for l in r.logs.iter() {
        if let Some(ix) = l.strip_prefix("Program log: Instruction: ") {
            tracing::info!("ix: {} {} [{}] {}", status, r.signature, i, ix);
            i += 1;
        }
    }
}
