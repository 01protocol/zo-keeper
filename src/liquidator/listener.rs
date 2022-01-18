use crate::{liquidator::accounts::DbWrapper, Error};
use anchor_client::solana_client::rpc_config::{
    RpcAccountInfoConfig, RpcProgramAccountsConfig,
};
use anchor_lang::Discriminator;
use bytemuck::Pod;
use futures::StreamExt;
use jsonrpc_core_client::transports::ws;
use solana_account_decoder::{UiAccountData, UiAccountEncoding};
use solana_rpc::rpc_pubsub::RpcSolPubSubClient;
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};
use std::str::FromStr;
use tracing::{debug, info, warn};
use zo_abi::{Cache, Control, Margin, State};

fn load_buf<T: Pod + Discriminator>(b: &[u8]) -> Option<&T> {
    match b.len() == 8 + std::mem::size_of::<T>()
        && b[..8] == T::discriminator()
    {
        false => None,
        true => bytemuck::try_from_bytes(&b[8..]).ok(),
    }
}

#[tracing::instrument(skip_all, level = "error", name = "listener")]
pub async fn start_listener(
    pid: &Pubkey,
    ws_url: String,
    db: DbWrapper,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let config = RpcProgramAccountsConfig {
        filters: None,
        account_config: RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64),
            data_slice: None,
            commitment: Some(CommitmentConfig::confirmed()),
        },
        with_context: Some(false),
    };

    loop {
        interval.tick().await;
        info!("connecting...");

        let sub = ws::try_connect::<RpcSolPubSubClient>(&ws_url)
            .unwrap()
            .await
            .and_then(|p| {
                p.program_subscribe(pid.to_string(), Some(config.clone()))
            });

        let mut sub = match sub {
            Ok(x) => x,
            Err(e) => {
                let e = Error::from(e);
                warn!("failed to connect: {0}: {0:?}", e);
                continue;
            }
        };

        while let Some(resp) = sub.next().await {
            let resp = match resp {
                Ok(x) => x,
                Err(e) => {
                    warn!("error: {0}: {0:?}", e);
                    continue;
                }
            };

            let buf = &match resp.value.account.data {
                UiAccountData::Binary(b, _) => base64::decode(b).unwrap(),
                _ => panic!(),
            };
            let pk = &resp.value.pubkey;

            if let Some(a) = load_buf::<Control>(buf) {
                debug!("got control data: {}", pk);
                let pk = Pubkey::from_str(pk).unwrap();
                db.get().lock().unwrap().update_control(pk, *a);
            } else if let Some(a) = load_buf::<Margin>(buf) {
                debug!("got margin data: {}", pk);
                let pk = Pubkey::from_str(pk).unwrap();
                db.get().lock().unwrap().update_margin(pk, *a);
            } else if let Some(a) = load_buf::<Cache>(buf) {
                debug!("got cache data: {}", pk);
                db.get().lock().unwrap().update_cache(*a);
            } else if let Some(a) = load_buf::<State>(buf) {
                debug!("got state data: {}", pk);
                db.get().lock().unwrap().update_state(*a);
            } else {
                debug!("unknown account type, skipping");
            }
        }

        warn!("disconnect");
    }
}
