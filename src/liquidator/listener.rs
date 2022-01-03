/*
 * This file is unused for now, but it's purpose is to avoid
 * having to fetch all the control at each step.
 * Instead, we listen to changes to program accounts, and only track that
*/

use anchor_lang::{Owner, ZeroCopy};

use jsonrpc_core::futures::StreamExt;
use jsonrpc_core_client::transports::ws;

use tokio::runtime::Runtime;
use tokio::sync::mpsc;

use solana_account_decoder::UiAccount;
use solana_rpc::rpc_pubsub::RpcSolPubSubClient;
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};

use std::mem;
use std::str::FromStr;

use tracing::{error_span, info, error};

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
                span.in_scope(|| error!(
                    "Got incorrect account data length: {}",
                    data_len
                ));
                Err(Self::Error::IncorrectData)
            }
        }
    }
}

pub fn start_listener(
    runtime: &Runtime,
    program_id: &Pubkey,
    ws_url: &String,
    db: Db,
    modulus: &u8,
    remainder: &u8,
) {
    let (tx, mut rx) = get_channel(1024);
    let modulus = modulus.clone();
    let remainder = remainder.clone();
    runtime.spawn(async move {
        let span = error_span!("listener");
        while let Some(cmd) = rx.recv().await {
            match cmd {
                Command::ControlChange { key, control } => {
                    span.in_scope(|| info!("Got control data {:?}", key));
                    if !is_right_remainder(&key, &modulus, &remainder) {
                        continue;
                    }
                    let mut db = db.lock().unwrap();
                    let control_acc: Control =
                        get_type_from_ui_account::<Control>(&key, &control);
                    db.update_control(&key, &control_acc);
                }
                Command::MarginChange { key, margin } => {
                    span.in_scope(|| info!("Got margin data {:?}", key));
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
                    span.in_scope(|| info!("Got cache data {:?}", key));
                    let mut db = db.lock().unwrap();
                    let cache_acc: Cache =
                        get_type_from_ui_account::<Cache>(&key, &cache);
                    db.update_cache(&cache_acc);
                }
                Command::StateChange { key, state } => {
                    span.in_scope(|| info!("Got state data {:?}", key));
                    let mut db = db.lock().unwrap();
                    let state_acc: State =
                        get_type_from_ui_account::<State>(&key, &state);
                    db.update_state(&state_acc);
                }
            }
        }
    });

    let id = program_id.clone();
    let url = ws_url.clone();
    let tx2 = tx.clone();
    runtime.spawn(async move {
        start_processor(&id, &url, tx2).await.unwrap();
    });
}

pub fn get_channel(
    buffer_size: usize,
) -> (mpsc::Sender<Command>, mpsc::Receiver<Command>) {
    mpsc::channel(buffer_size)
}

// Should have a fn for listening, one for processing.
async fn start_processor(
    program_id: &Pubkey,
    ws_endpoint: &String,
    tx: mpsc::Sender<Command>,
) -> Result<(), ErrorCode> {
    let connection = ws::try_connect::<RpcSolPubSubClient>(ws_endpoint)
        .map_err(|_| ErrorCode::ConnectionFailure)?;
    let client = connection.await.map_err(|_| ErrorCode::EndpointFailure)?;

    // Now for configuring accounts we care about
    let control_tx = tx.clone();
    let control_listener =
        spawn_listener::<Control>(program_id, &client, control_tx)?;

    let margin_tx = tx.clone();
    let margin_listener =
        spawn_listener::<Margin>(program_id, &client, margin_tx)?;

    let state_tx = tx.clone();
    let state_listener =
        spawn_listener::<State>(program_id, &client, state_tx)?;

    let cache_tx = tx.clone();
    let cache_listener =
        spawn_listener::<Cache>(program_id, &client, cache_tx)?;

    control_listener.await.unwrap();
    margin_listener.await.unwrap();
    state_listener.await.unwrap();
    cache_listener.await.unwrap();

    Ok(())
}

fn spawn_listener<'a, T: ZeroCopy + Owner + Sync + Send>(
    program_id: &Pubkey,
    client: &RpcSolPubSubClient,
    tx: mpsc::Sender<Command>,
) -> Result<tokio::task::JoinHandle<()>, ErrorCode> {
    let data_size: usize = mem::size_of::<T>();

    let accounts_config = get_program_account_config(
        data_size as u64,
        CommitmentConfig::confirmed(),
    );
    let mut subscriber = client
        .program_subscribe(program_id.to_string(), Some(accounts_config))
        .map_err(|_| ErrorCode::SubscriptionFailure)?;

    let tx = tx.clone();
    Ok(tokio::spawn(async move {
        let span = error_span!("listener");
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
                            span.in_scope(|| error!(
                                "Got incorrect account data: {:?}, for account {}.",
                                err,
                                key
                            ));
                            continue;
                        }
                    };
                    let cmd: Command = update_key(command, key);
                    match tx.send(cmd).await {
                        Ok(()) => (),
                        Err(_) => {
                            span.in_scope(|| error!("Error sending command"));
                            break;
                        }
                    }
                }
                Err(_) => span.in_scope(|| error!(
                    "No {} account here",
                    std::any::type_name::<T>()
                )),
            }
        }
        span.in_scope(|| error!(
            "{} stream closed. Panicking!",
            std::any::type_name::<T>()
        ));
        panic!();
    }))
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
