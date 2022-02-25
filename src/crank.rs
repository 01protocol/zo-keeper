use crate::{error::Error, AppState};
use anchor_client::solana_sdk::instruction::AccountMeta;
use std::{cmp::min, marker::Send, sync::Arc, time::Duration};
use tokio::time::{Interval, MissedTickBehavior};
use tracing::{info, warn};

pub struct CrankConfig {
    pub cache_oracle_interval: Duration,
    pub cache_interest_interval: Duration,
    pub update_funding_interval: Duration,
}

pub async fn run(st: &'static AppState, cfg: CrankConfig) -> Result<(), Error> {
    let cache_oracle_tasks = st
        .iter_oracles()
        .collect::<Vec<_>>()
        .chunks(16)
        .map(|x| {
            let (symbols, accounts): (Vec<String>, Vec<AccountMeta>) = x
                .iter()
                .map(|x| {
                    let symbol = x.symbol.into();
                    let acc =
                        AccountMeta::new_readonly(x.sources[0].key, false);
                    (symbol, acc)
                })
                .unzip();

            let symbols = Arc::new(symbols);
            let accounts = Arc::new(accounts);

            loop_blocking(interval(cfg.cache_oracle_interval), move || {
                cache_oracle(st, &symbols, &accounts)
            })
        })
        .collect::<Vec<_>>();

    let cache_interest_tasks = (0..st.zo_state.total_collaterals as u8)
        .step_by(8)
        .map(|i| {
            let start = i;
            let end = min(i + 4, st.zo_state.total_collaterals as u8);

            loop_blocking(interval(cfg.cache_interest_interval), move || {
                cache_interest(st, start, end)
            })
        });

    let update_funding_tasks = st.load_dex_markets().map(|(symbol, market)| {
        let symbol = Arc::new(symbol);
        let market = Arc::new(market);

        loop_blocking(interval(cfg.update_funding_interval), move || {
            update_funding(st, &symbol, &market)
        })
    });

    futures::join!(
        futures::future::join_all(cache_oracle_tasks),
        futures::future::join_all(cache_interest_tasks),
        futures::future::join_all(update_funding_tasks),
    );

    Ok(())
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
        tokio::task::spawn_blocking(f.clone());
    }
}

#[tracing::instrument(skip_all, level = "error", fields(symbols = ?s))]
fn cache_oracle(st: &AppState, s: &[String], accs: &[AccountMeta]) {
    let program = st.program();
    let req = program
        .request()
        .args(zo_abi::instruction::CacheOracle {
            symbols: s.to_owned(),
            mock_prices: None,
        })
        .accounts(zo_abi::accounts::CacheOracle {
            signer: st.payer(),
            cache: st.zo_cache_pubkey,
        });

    let req = accs.iter().fold(req, |r, x| r.accounts(x.clone()));

    match req.send() {
        Ok(sg) => info!("{}", sg),
        Err(e) => {
            let e = Error::from(e);
            warn!("{}", e);
        }
    };
}

#[tracing::instrument(skip_all, level = "error", fields(from = start, to = end))]
fn cache_interest(st: &AppState, start: u8, end: u8) {
    let program = st.program();
    let res = program
        .request()
        .args(zo_abi::instruction::CacheInterestRates { start, end })
        .accounts(zo_abi::accounts::CacheInterestRates {
            signer: st.payer(),
            state: st.zo_state_pubkey,
            cache: st.zo_cache_pubkey,
        })
        .send();

    match res {
        Ok(sg) => info!("{}", sg),
        Err(e) => {
            let e = Error::from(e);
            warn!("{}", e);
        }
    };
}

#[tracing::instrument(skip_all, level = "error", fields(symbol = symbol))]
fn update_funding(st: &AppState, symbol: &str, m: &zo_abi::dex::ZoDexMarket) {
    let program = st.program();
    let res = program
        .request()
        .args(zo_abi::instruction::UpdatePerpFunding {})
        .accounts(zo_abi::accounts::UpdatePerpFunding {
            state: st.zo_state_pubkey,
            state_signer: st.zo_state_signer_pubkey,
            cache: st.zo_cache_pubkey,
            dex_market: m.own_address,
            market_bids: m.bids,
            market_asks: m.asks,
            dex_program: zo_abi::ZO_DEX_PID,
        })
        .send();

    match res {
        Ok(sg) => info!("{}", sg),
        Err(e) => {
            let e = Error::from(e);
            warn!("{}", e);
        }
    };
}
