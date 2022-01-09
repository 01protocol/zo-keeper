mod events;

use crate::{
    db,
    {error::Error, AppState},
};
use anchor_client::solana_client::rpc_config::{
    RpcAccountInfoConfig, RpcTransactionLogsConfig, RpcTransactionLogsFilter,
};
use futures::StreamExt;
use jsonrpc_core_client::transports::ws;
use solana_account_decoder::{UiAccountData, UiAccountEncoding};
use solana_rpc::rpc_pubsub::RpcSolPubSubClient;
use std::{
    collections::HashMap,
    env,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tracing::{debug, error_span, info, warn, Instrument};

pub async fn run(st: &'static AppState) -> Result<(), Error> {
    let db_client =
        mongodb::Client::with_uri_str(env::var("DATABASE_URL")?).await?;

    let db = db_client.database("main");
    let db: &'static _ = Box::leak(Box::new(db));

    futures::join!(
        scrape_logs(st, db),
        listen_event_queue(st, db),
        poll_update_funding(st, db),
    );

    Ok(())
}

#[tracing::instrument(skip_all, level = "error", name = "scrape_logs")]
async fn scrape_logs(st: &'static AppState, db: &'static mongodb::Database) {
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

            tokio::spawn(events::process(
                st,
                db,
                resp.value.logs,
                resp.value.signature,
            ));
        }
    }
}

async fn listen_event_queue(
    st: &'static AppState,
    db: &'static mongodb::Database,
) {
    let handles: Vec<_> = st
        .load_dex_markets()
        .map(|(symbol, dex_market)| {
            let base_decimals = dex_market.coin_decimals as u8;
            let quote_decimals = 6u8;
            let event_q = dex_market.event_q.to_string();

            tokio::spawn(async move {
                let span = error_span!("event_queue", symbol = symbol.as_str());

                loop {
                    let event_q = event_q.clone();

                    let sub = ws::try_connect::<RpcSolPubSubClient>(
                        st.cluster.ws_url(),
                    )
                    .unwrap()
                    .await
                    .and_then(|p| {
                        p.account_subscribe(
                            event_q,
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
                        span.in_scope(|| info!("got update"));

                        let resp = match resp {
                            Ok(x) => x,
                            Err(e) => {
                                let e = Error::from(e);
                                warn!("{}", e);
                                continue;
                            }
                        };

                        let buf = match resp.value.data {
                            UiAccountData::Binary(b, _) => {
                                base64::decode(b).unwrap()
                            }
                            _ => panic!(),
                        };

                        let db_res = db::Trade::update(
                            db,
                            &symbol,
                            base_decimals,
                            quote_decimals,
                            &buf,
                        )
                        .instrument(span.clone())
                        .await;

                        if let Err(e) = db_res {
                            let e = Error::from(e);
                            warn!("{}", e);
                        }
                    }
                }
            })
        })
        .collect();

    let _ = futures::future::join_all(handles).await;
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

    let prev: &'static _ = Box::leak(Box::new(prev));

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
