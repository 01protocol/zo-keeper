use crate::{db, error::Error, AppState};
use anchor_client::{
    solana_client::rpc_config::{
        RpcTransactionConfig, RpcTransactionLogsConfig,
        RpcTransactionLogsFilter,
    },
    solana_sdk::{commitment_config::CommitmentConfig, signature::Signature},
};
use futures::StreamExt;
use jsonrpc_core_client::transports::ws;
use solana_rpc::rpc_pubsub::RpcSolPubSubClient;
use solana_transaction_status::UiTransactionEncoding;
use std::{
    cell::Cell,
    collections::HashMap,
    env,
    time::{Duration, SystemTime},
};
use tracing::{debug, info, trace, warn, Instrument};

#[cfg(not(feature = "devnet"))]
static DB_NAME: &str = "keeper";

#[cfg(feature = "devnet")]
static DB_NAME: &str = "keeper-devnet";

pub async fn run(st: &'static AppState) -> Result<(), Error> {
    let db = mongodb::Client::with_uri_str(env::var("DATABASE_URL")?)
        .await?
        .database(DB_NAME);

    let db: &'static _ = Box::leak(Box::new(db));

    futures::join!(
        listen_logs(st, db),
        poll_logs(st, db),
        poll_update_funding(st, db),
        poll_open_interest(st, db),
    );

    Ok(())
}

#[tracing::instrument(skip_all, level = "error")]
async fn listen_logs(st: &'static AppState, db: &'static mongodb::Database) {
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        // On disconnect, retry every 5s.
        interval.tick().await;

        let sub =
            match ws::try_connect::<RpcSolPubSubClient>(st.cluster.ws_url()) {
                Ok(x) => x.await,
                Err(e) => Err(e),
            }
            .and_then(|p| {
                p.logs_subscribe(
                    RpcTransactionLogsFilter::Mentions(vec![
                        zo_abi::ID.to_string()
                    ]),
                    Some(RpcTransactionLogsConfig {
                        commitment: Some(CommitmentConfig::finalized()),
                    }),
                )
            });

        let mut sub = match sub {
            Ok(x) => x,
            Err(e) => {
                let e = Error::from(e);
                warn!("{}", e);
                continue;
            }
        };

        while let Some(resp) = sub.next().await {
            let resp = match resp {
                Ok(x) => x,
                Err(_) => continue,
            };

            if resp.value.err.is_some() {
                continue;
            }

            let time = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;

            tokio::spawn(
                crate::events::process(
                    st,
                    db,
                    resp.value.logs,
                    resp.value.signature,
                    time,
                )
                .instrument(tracing::Span::current()),
            );
        }
    }
}

#[tracing::instrument(skip_all, level = "error")]
async fn poll_logs(st: &'static AppState, db: &'static mongodb::Database) {
    let mut interval = tokio::time::interval(Duration::from_millis(250));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut last_slot: u64 = st
        .rpc
        .get_account_with_commitment(
            &st.zo_state_pubkey,
            CommitmentConfig::confirmed(),
        )
        .unwrap()
        .context
        .slot;

    loop {
        interval.tick().await;

        // > The result field will be an array of transaction signature
        // > information, ordered from newest to oldest transaction.
        //
        // https://docs.solana.com/developing/clients/jsonrpc-api#getsignaturesforaddress
        let sigs = tokio::task::spawn_blocking(move || {
            st.rpc.get_signatures_for_address(&st.zo_state_pubkey)
        })
        .await
        .unwrap();

        let sigs = match sigs {
            Ok(x) => x
                .into_iter()
                .take(200)
                .filter(|sg| sg.err.is_none() && sg.slot > last_slot)
                .collect::<Vec<_>>(),
            Err(e) => {
                let e = Error::from(e);
                warn!("{}", e);
                continue;
            }
        };

        if sigs.is_empty() {
            trace!("0 signatures, skipping");
            continue;
        }

        debug!("processing {} signatures", sigs.len());

        let handle = tokio::runtime::Handle::try_current().unwrap();
        let span = tracing::Span::current();

        let time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        for sg in sigs {
            let handle = handle.clone();
            let span = span.clone();

            last_slot = std::cmp::max(last_slot, sg.slot);

            tokio::task::spawn_blocking(move || {
                use std::str::FromStr;
                let _g = span.enter();
                debug!("processing: {}", sg.signature);

                // The signatures are received with "finalized" commitment,
                // and the transaction itself is received with "confirmed".
                // This avoid the issue where the transaction returns null
                // sometimes even though the signature is finalized.
                let res = st.rpc.get_transaction_with_config(
                    &Signature::from_str(&sg.signature).unwrap(),
                    RpcTransactionConfig {
                        encoding: Some(UiTransactionEncoding::Base64),
                        commitment: Some(CommitmentConfig::confirmed()),
                        max_supported_transaction_version: None,
                    },
                );

                match res {
                    Ok(tx) => {
                        if let Some(ss) =
                            tx.transaction.meta.and_then(|x| x.log_messages)
                        {
                            handle.block_on(
                                crate::events::process(
                                    st,
                                    db,
                                    ss,
                                    sg.signature,
                                    time,
                                )
                                .instrument(span.clone()),
                            );
                        }
                    }
                    Err(e) => {
                        let e = Error::from(e);
                        warn!("{}", e);
                        return;
                    }
                };
            });
        }
    }
}

#[tracing::instrument(skip_all, level = "error", name = "update_funding")]
async fn poll_update_funding(
    st: &'static AppState,
    db: &'static mongodb::Database,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Previous update funding time. The funding is only
    // inserted into the DB if the funding time increases.
    let prev: HashMap<String, Cell<zo_abi::dex::ZoDexMarket>> = st
        .load_dex_markets()
        .unwrap()
        .into_iter()
        .map(|(s, m)| (s, Cell::new(m)))
        .collect();

    loop {
        interval.tick().await;

        let markets = match st.load_dex_markets() {
            Ok(x) => x,
            Err(e) => {
                warn!("{}", Error::from(e));
                continue;
            }
        };

        let to_update: Vec<_> = markets
            .into_iter()
            .zip(st.iter_markets())
            .filter_map(|((symbol, m), p)| {
                let prev_m = prev.get(&symbol).map(|x| x.get()).unwrap();

                match m.last_updated > prev_m.last_updated {
                    true => Some((symbol, m, prev_m, p)),
                    false => None,
                }
            })
            .collect();

        if to_update.is_empty() {
            debug!("nothing to update");
            continue;
        }

        let new_entries: Vec<_> = to_update
            .iter()
            .map(|(symbol, m, prev_m, p)| {
                use fixed::types::I80F48;

                // small/big
                let delta =
                    I80F48::from_num(m.funding_index - prev_m.funding_index);

                // small/small
                let price: I80F48 = st
                    .iter_oracles()
                    .find(|o| o.symbol == p.oracle_symbol)
                    .unwrap()
                    .price
                    .into();

                // small/small -> big/big
                let mut price = match p.asset_decimals >= 6 {
                    true => {
                        price
                            * I80F48::from(
                                10u64.pow(p.asset_decimals as u32 - 6),
                            )
                    }
                    false => {
                        price
                            / I80F48::from(
                                10u64.pow(6 - p.asset_decimals as u32),
                            )
                    }
                };

                if p.perp_type == zo_abi::PerpType::Square {
                    price = price * price / I80F48::from(p.strike);
                }

                // big/big -> small/big
                price *= I80F48::from(10u64.pow(6));

                db::Funding {
                    symbol: symbol.clone(),
                    funding_index: { m.funding_index }.to_string(),
                    hourly: (delta / price).to_num::<f64>(),
                    time: m.last_updated as i64,
                }
            })
            .collect();

        if let Err(e) = db::Funding::update(db, &new_entries).await {
            let e = Error::from(e);
            warn!("{}", e);
            continue;
        }

        let updated: Vec<_> =
            to_update.iter().map(|(s, _, _, _)| s).cloned().collect();

        for (s, m, _, _) in to_update.into_iter() {
            prev.get(&s).unwrap().set(m);
        }

        info!("inserted {}", updated.join(", "));
    }
}

#[tracing::instrument(skip_all, level = "error", name = "open_interest")]
async fn poll_open_interest(
    st: &'static AppState,
    db: &'static mongodb::Database,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(300));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        interval.tick().await;

        let time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let val: Result<_, Error> = tokio::task::spawn_blocking(move || {
            let mut r = vec![0i64; st.zo_state.total_markets as usize];

            crate::utils::load_program_accounts::<zo_abi::Control>(&st.rpc)?
                .into_iter()
                .for_each(|(_, a)| {
                    for (i, e) in r.iter_mut().enumerate() {
                        let x = a.open_orders_agg[i].pos_size;
                        if x > 0 {
                            *e += x;
                        }
                    }
                });

            Ok(st
                .iter_markets()
                .enumerate()
                .map(|(i, m)| (m.symbol.into(), r[i]))
                .collect::<HashMap<String, i64>>())
        })
        .await
        .unwrap();

        let val = match val {
            Ok(x) => x,
            Err(e) => {
                warn!("{}", e);
                continue;
            }
        };

        if let Err(e) = db::OpenInterest::insert(db, time, val).await {
            let e = Error::from(e);
            warn!("{}", e);
        }
    }
}
