/*
 * This file is unused for now, but it's purpose is to avoid
 * having to fetch all the control at each step.
 * Instead, we listen to changes to program accounts, and only track that
*/

use anchor_lang::{Owner, ZeroCopy};

use jsonrpc_core::futures::StreamExt;
use jsonrpc_core_client::transports::ws;

use tokio::{runtime::Runtime, sync::mpsc};

use solana_account_decoder::UiAccount;
use solana_rpc::rpc_pubsub::RpcSolPubSubClient;
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};

use std::{mem, str::FromStr};

use tracing::{debug, error, error_span, info, warn};

use zo_abi::{Cache, Control, Margin, State};

use crate::liquidator::{accounts::Db, error::ErrorCode, utils::*};

pub enum Command {
    ControlChange { key: Pubkey, control: UiAccount },
    MarginChange { key: Pubkey, margin: UiAccount },
    CacheChange { key: Pubkey, cache: UiAccount },
    StateChange { key: Pubkey, state: UiAccount },
}

impl TryFrom<UiAccount> for Command {
    type Error = ErrorCode;

    fn try_from(account: UiAccount) -> Result<Self, Self::Error> {
        let data_len = vec_from_data(account.data.clone()).len();

        let account_key = Pubkey::from_str(&account.owner).unwrap();
        let span = error_span!("try_from", account_key = %account_key);
        match data_len - 8 {
            x if x == mem::size_of::<Control>() => Ok(Command::ControlChange {
                key: account_key,
                control: account,
            }),
            x if x == mem::size_of::<Margin>() => Ok(Command::MarginChange {
                key: account_key,
                margin: account,
            }),
            x if x == mem::size_of::<Cache>() => Ok(Command::CacheChange {
                key: account_key,
                cache: account,
            }),
            x if x == mem::size_of::<State>() => Ok(Command::StateChange {
                key: account_key,
                state: account,
            }),
            _ => {
                span.in_scope(|| {
                    error!("Got incorrect account data length: {}", data_len)
                });
                Err(Self::Error::IncorrectData)
            }
        }
    }
}

pub fn start_listener(
    runtime: &Runtime,
    program_id: &Pubkey,
    ws_url: &str,
    db: Db,
    modulus: &u8,
    remainder: &u8,
) {
    let span = error_span!("listener");
    span.in_scope(|| info!("starting..."));
    let (tx, mut rx) = mpsc::channel::<Command>(1024);
    let modulus = *modulus;
    let remainder = *remainder;
    runtime.spawn(async move {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                Command::ControlChange { key, control } => {
                    span.in_scope(|| debug!("Got control data {:?}", key));
                    if !is_right_remainder(&key, &modulus, &remainder) {
                        continue;
                    }
                    let mut db = db.lock().unwrap();
                    let control_acc: Control =
                        get_type_from_ui_account::<Control>(&key, &control);
                    db.update_control(&key, &control_acc);
                }
                Command::MarginChange { key, margin } => {
                    span.in_scope(|| debug!("Got margin data {:?}", key));
                    let margin_acc: Margin =
                        get_type_from_ui_account::<Margin>(&key, &margin);
                    if !is_right_remainder(
                        &margin_acc.control,
                        &modulus,
                        &remainder,
                    ) {
                        continue;
                    }
                    let mut db = db.lock().unwrap();
                    db.update_margin(&key, &margin_acc);
                }
                Command::CacheChange { key, cache } => {
                    span.in_scope(|| debug!("Got cache data {:?}", key));
                    let mut db = db.lock().unwrap();
                    let cache_acc: Cache =
                        get_type_from_ui_account::<Cache>(&key, &cache);
                    db.update_cache(&cache_acc);
                }
                Command::StateChange { key, state } => {
                    span.in_scope(|| debug!("Got state data {:?}", key));
                    let mut db = db.lock().unwrap();
                    let state_acc: State =
                        get_type_from_ui_account::<State>(&key, &state);
                    db.update_state(&state_acc);
                }
            }
        }
    });

    let id = *program_id;
    let url = ws_url.to_string();
    let tx2 = tx;
    runtime.spawn(async move {
        start_processor(&id, &url, tx2).await;
    });
}

// Should have a fn for listening, one for processing.
async fn start_processor(
    program_id: &Pubkey,
    ws_endpoint: &str,
    tx: mpsc::Sender<Command>,
) -> ! {
    let client = loop {
        match ws::try_connect::<RpcSolPubSubClient>(ws_endpoint) {
            Ok(x) => match x.await {
                Ok(x) => break x,
                Err(e) => {
                    warn!("{0}: {0:?}", e);
                    continue;
                }
            },
            Err(e) => {
                warn!("{0}: {0:?}", e);
                continue;
            }
        }
    };

    tokio::join!(
        spawn_listener::<Control>(program_id, &client, tx.clone()),
        spawn_listener::<Margin>(program_id, &client, tx.clone()),
        spawn_listener::<State>(program_id, &client, tx.clone()),
        spawn_listener::<Cache>(program_id, &client, tx),
    );

    unreachable!()
}

#[tracing::instrument(
    skip_all,
    level = "error",
    fields(ty = %std::any::type_name::<T>())
)]
async fn spawn_listener<T: ZeroCopy + Owner + Sync + Send>(
    program_id: &Pubkey,
    client: &RpcSolPubSubClient,
    tx: mpsc::Sender<Command>,
) {
    let data_size: usize = mem::size_of::<T>();

    let accounts_config = get_program_account_config(
        data_size as u64,
        CommitmentConfig::confirmed(),
    );

    loop {
        debug!("connecting...");

        let subscriber = client.program_subscribe(
            program_id.to_string(),
            Some(accounts_config.clone()),
        );

        let mut subscriber = match subscriber {
            Ok(x) => x,
            Err(e) => {
                warn!("failed to connect: {0}: {0:?}", e);
                continue;
            }
        };

        while let Some(result) = subscriber.next().await {
            match result {
                Ok(response) => {
                    let keyed_account = response.value;
                    let account = keyed_account.account;
                    let key: Pubkey =
                        Pubkey::from_str(&keyed_account.pubkey).unwrap();
                    let command: Command = match account.try_into() {
                        Ok(cmd) => cmd,
                        Err(err) => {
                            error!(
                                "Got incorrect account data: {:?}, for account {}.",
                                err,
                                key
                            );
                            continue;
                        }
                    };
                    let cmd: Command = update_key(command, key);
                    match tx.send(cmd).await {
                        Ok(()) => (),
                        Err(_) => {
                            error!("Error sending command");
                            break;
                        }
                    }
                }
                Err(_) => debug!("wrong account type"),
            }
        }

        warn!("disconnect");
    }
}

fn update_key(command: Command, new_key: Pubkey) -> Command {
    match command {
        Command::ControlChange { key: _, control } => Command::ControlChange {
            key: new_key,
            control,
        },
        Command::MarginChange { key: _, margin } => Command::MarginChange {
            key: new_key,
            margin,
        },
        Command::CacheChange { key: _, cache } => Command::CacheChange {
            key: new_key,
            cache,
        },
        Command::StateChange { key: _, state } => Command::StateChange {
            key: new_key,
            state,
        },
    }
}
