use crate::{db, error::Error, AppState};
use anchor_client::{
    solana_client::rpc_config::{
        RpcAccountInfoConfig, RpcTransactionLogsConfig,
        RpcTransactionLogsFilter,
    },
    solana_sdk::{commitment_config::CommitmentConfig, signature::Signature},
};
use futures::{StreamExt, TryFutureExt};
use jsonrpc_core_client::transports::ws;
use solana_account_decoder::{UiAccountData, UiAccountEncoding};
use solana_rpc::rpc_pubsub::RpcSolPubSubClient;
use solana_transaction_status::UiTransactionEncoding;
use std::{
    collections::HashMap,
    env,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant, SystemTime},
};
use tracing::{debug, info, warn, Instrument};

#[cfg(not(feature = "devnet"))]
static DB_NAME: &str = "keeper";

#[cfg(feature = "devnet")]
static DB_NAME: &str = "keeper-devnet";

pub async fn run(st: &'static AppState) -> Result<(), Error> {
    let db = mongodb::Client::with_uri_str(env::var("DATABASE_URL")?)
        .await?
        .database(DB_NAME);

    let db: &'static _ = Box::leak(Box::new(db));

    let listen_event_q_tasks =
        st.load_dex_markets().map(|(symbol, dex_market)| {
            listen_event_queue(st, db, symbol, dex_market)
        });

    futures::join!(
        listen_logs(st, db),
        poll_logs(st, db),
        poll_update_funding(st, db),
        poll_open_interest(st, db),
        futures::future::join_all(listen_event_q_tasks),
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

        let sub = ws::try_connect::<RpcSolPubSubClient>(st.cluster.ws_url())
            .unwrap()
            .await
            .and_then(|p| {
                p.logs_subscribe(
                    RpcTransactionLogsFilter::Mentions(vec![
                        zo_abi::ID.to_string()
                    ]),
                    Some(RpcTransactionLogsConfig { commitment: None }),
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

            tokio::spawn(
                crate::events::process(
                    st,
                    db,
                    resp.value.logs,
                    resp.value.signature,
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
        let t = Instant::now();

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
            Ok(x) => x,
            Err(e) => {
                let e = Error::from(e);
                warn!("{}", e);
                continue;
            }
        };

        let txs = sigs
            .into_iter()
            .take(200)
            .filter(|sg| sg.err.is_none() && sg.slot > last_slot)
            .map(|sg| {
                tokio::task::spawn_blocking(move || {
                    use std::str::FromStr;

                    st.rpc
                        .get_transaction(
                            &Signature::from_str(&sg.signature).unwrap(),
                            UiTransactionEncoding::Base64,
                        )
                        .map(|x| (sg.signature, x))
                })
            });

        let txs: Result<Vec<_>, _> = futures::future::try_join_all(txs)
            .await
            .unwrap()
            .into_iter()
            .collect();

        let txs = match txs {
            Ok(l) => l,
            Err(e) => {
                let e = Error::from(e);
                warn!("{}", e);
                continue;
            }
        };

        let span = tracing::Span::current();

        debug!(
            "parsing {} txs after {}ms",
            txs.len(),
            t.elapsed().as_millis()
        );

        for (sg, l) in txs.into_iter() {
            if let Some(ss) = l.transaction.meta.and_then(|x| x.log_messages) {
                tokio::spawn(
                    crate::events::process(st, db, ss, sg)
                        .instrument(span.clone()),
                );

                last_slot = std::cmp::max(last_slot, l.slot);
            }
        }
    }
}

#[tracing::instrument(
    skip_all,
    level = "error",
    name = "event_queue",
    fields(symbol = %symbol)
)]
async fn listen_event_queue(
    st: &'static AppState,
    db: &'static mongodb::Database,
    symbol: String,
    mkt: zo_abi::dex::ZoDexMarket,
) {
    let symbol = std::sync::Arc::new(symbol);
    let event_q = mkt.event_q.to_string();
    let base_decimals = mkt.coin_decimals as u8;
    let quote_decimals = 6u8;

    loop {
        let sub = ws::try_connect::<RpcSolPubSubClient>(st.cluster.ws_url())
            .unwrap()
            .await
            .and_then(|p| {
                p.account_subscribe(
                    event_q.clone(),
                    Some(RpcAccountInfoConfig {
                        encoding: Some(UiAccountEncoding::Base64),
                        data_slice: None,
                        commitment: None,
                    }),
                )
            });

        let mut sub = match sub {
            Err(e) => {
                let e = Error::from(e);
                warn!("{}", e);
                continue;
            }
            Ok(x) => x,
        };

        while let Some(resp) = sub.next().await {
            debug!("got update");

            let resp = match resp {
                Ok(x) => x,
                Err(_) => continue,
            };

            let buf = match resp.value.data {
                UiAccountData::Binary(b, _) => base64::decode(b).unwrap(),
                _ => panic!(),
            };

            let symbol = symbol.clone();
            let span = tracing::Span::current();

            tokio::spawn(async move {
                db::Trade::update(
                    db,
                    &symbol,
                    base_decimals,
                    quote_decimals,
                    &buf,
                )
                .instrument(span)
                .map_err(|e| {
                    let e = Error::from(e);
                    warn!("{}", e);
                })
                .await
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
    let prev: HashMap<String, AtomicU64> = st
        .load_dex_markets()
        .map(|(s, _)| (s, AtomicU64::new(0)))
        .collect();

    loop {
        interval.tick().await;

        let to_update: Vec<_> = st
            .load_dex_markets()
            .filter(|(symbol, m)| {
                let prev_update = prev
                    .get(symbol)
                    .map(|x| x.load(Ordering::Relaxed))
                    .unwrap();

                m.last_updated > prev_update
            })
            .collect();

        if to_update.is_empty() {
            debug!("nothing to update");
            continue;
        }

        let new_entries: Vec<_> = to_update
            .iter()
            .map(|(symbol, m)| db::Funding {
                symbol: symbol.clone(),
                funding_index: { m.funding_index }.to_string(),
                last_updated: m.last_updated as i64,
            })
            .collect();

        if let Err(e) = db::Funding::update(db, &new_entries).await {
            let e = Error::from(e);
            warn!("{}", e);
            continue;
        }

        let updated: Vec<_> =
            to_update.iter().map(|(s, _)| s).cloned().collect();

        for (s, m) in to_update.into_iter() {
            prev.get(&s)
                .unwrap()
                .store(m.last_updated, Ordering::Relaxed);
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

        let val = tokio::task::spawn_blocking(move || {
            let mut r = vec![0i64; st.zo_state.total_markets as usize];

            crate::utils::load_program_accounts::<zo_abi::Control>(&st.rpc)
                .unwrap()
                .into_iter()
                .for_each(|(_, a)| {
                    for (i, e) in r.iter_mut().enumerate() {
                        let x = a.open_orders_agg[i].pos_size;
                        if x > 0 {
                            *e += x;
                        }
                    }
                });

            st.iter_markets()
                .enumerate()
                .map(|(i, m)| (m.symbol.into(), r[i]))
                .collect::<HashMap<String, i64>>()
        })
        .await;

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
