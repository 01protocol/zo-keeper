use crate::{error::Error, AppState};
use anchor_client::solana_sdk::instruction::AccountMeta;
use itertools::Itertools;
use std::{cmp::min, env, marker::Send, str::FromStr, time::Duration};
use tokio::time::{Interval, MissedTickBehavior};
use tracing::{debug, warn};

pub async fn run(st: &'static AppState) -> Result<(), Error> {
    let cache_oracle_duration =
        load_env_duration("ZO_CACHE_ORACLE_INTERVAL_MS", 2000);

    let cache_oracle_tasks = st
        .iter_oracles()
        .chunks(4)
        .into_iter()
        .map(|x| {
            let (symbols, accounts): (Vec<String>, Vec<AccountMeta>) = x
                .map(|x| {
                    let symbol = x.symbol.into();
                    let acc =
                        AccountMeta::new_readonly(x.sources[0].key, false);
                    (symbol, acc)
                })
                .unzip();

            let symbols: &'static _ = Box::leak(Box::new(symbols));
            let accounts: &'static _ = Box::leak(Box::new(accounts));

            loop_blocking(interval(cache_oracle_duration.clone()), move || {
                cache_oracle(st, symbols, accounts)
            })
        })
        .collect::<Vec<_>>();

    let cache_interest_duration =
        load_env_duration("ZO_CACHE_INTEREST_INTERVAL_MS", 5000);

    let cache_interest_tasks = (0..st.zo_state.total_collaterals as u8)
        .step_by(4)
        .map(|i| {
            let start = i;
            let end = min(i + 4, st.zo_state.total_collaterals as u8);

            loop_blocking(
                interval(cache_interest_duration.clone()),
                move || cache_interest(st, start, end),
            )
        });

    let update_funding_duration =
        load_env_duration("ZO_UPDATE_FUNDING_INTERVAL_MS", 15000);

    let update_funding_tasks = st.load_dex_markets().map(|(symbol, market)| {
        let symbol: &'static _ = Box::leak(symbol.into_boxed_str());
        let market: &'static _ = Box::leak(Box::new(market));

        loop_blocking(interval(update_funding_duration.clone()), move || {
            update_funding(st, symbol, market)
        })
    });

    futures::join!(
        futures::future::join_all(cache_oracle_tasks),
        futures::future::join_all(cache_interest_tasks),
        futures::future::join_all(update_funding_tasks),
    );

    Ok(())
}

fn load_env_duration(s: &str, default: u64) -> Duration {
    let ms = match env::var(s) {
        Ok(x) => u64::from_str(&x)
            .unwrap_or_else(|_| panic!("Failed to parse ${}", s)),
        Err(_) => default,
    };
    Duration::from_millis(ms)
}

fn interval(d: Duration) -> Interval {
    let mut interval = tokio::time::interval(d);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval
}

async fn loop_blocking<F>(mut interval: Interval, f: F)
where
    F: Fn() + Send + Clone + 'static,
{
    loop {
        interval.tick().await;
        if let Err(e) = tokio::task::spawn_blocking(f.clone()).await {
            if e.is_panic() {
                warn!("task panicked: {:?}", e);
            }
        }
    }
}

#[tracing::instrument(skip_all, level = "error", fields(symbols = ?s))]
fn cache_oracle(st: &AppState, s: &[String], accs: &[AccountMeta]) {
    let req = st
        .program
        .request()
        .args(zo_abi::instruction::CacheOracle {
            symbols: s.to_owned(),
            mock_prices: None,
        })
        .accounts(zo_abi::accounts::CacheOracle {
            signer: st.program.payer(),
            cache: st.zo_cache_pubkey,
        });

    let req = accs.iter().fold(req, |r, x| r.accounts(x.clone()));

    match req.send() {
        Ok(sg) => debug!("{}", sg),
        Err(e) => warn!("{}", e),
    };
}

#[tracing::instrument(skip_all, level = "error", fields(from = start, to = end))]
fn cache_interest(st: &AppState, start: u8, end: u8) {
    let res = st
        .program
        .request()
        .args(zo_abi::instruction::CacheInterestRates { start, end })
        .accounts(zo_abi::accounts::CacheInterestRates {
            signer: st.program.payer(),
            state: st.zo_state_pubkey,
            cache: st.zo_cache_pubkey,
        })
        .send();

    match res {
        Ok(sg) => debug!("{}", sg),
        Err(e) => warn!("{}", e),
    };
}

#[tracing::instrument(skip_all, level = "error", fields(symbol = symbol))]
fn update_funding(st: &AppState, symbol: &str, m: &zo_abi::dex::ZoDexMarket) {
    let res = st
        .program
        .request()
        .args(zo_abi::instruction::UpdatePerpFunding {})
        .accounts(zo_abi::accounts::UpdatePerpFunding {
            state: st.zo_state_pubkey,
            state_signer: st.zo_state_signer_pubkey,
            cache: st.zo_cache_pubkey,
            dex_market: m.own_address,
            market_bids: m.bids,
            market_asks: m.asks,
            dex_program: zo_abi::dex::ID,
        })
        .send();

    match res {
        Ok(sg) => debug!("{}", sg),
        Err(e) => warn!("{}", e),
    };
}
