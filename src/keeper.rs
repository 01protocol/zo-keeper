use crate::AppState;
use anchor_client::{
    anchor_lang::prelude::AccountMeta, solana_sdk::pubkey::Pubkey,
};
use std::{cmp::min, env, str::FromStr, time::Duration};
use tokio::time::Interval;

pub async fn run(st: &'static AppState) {
    let cache_oracle_interval =
        load_env_interval("ZO_CACHE_ORACLE_INTERVAL_MS", 1000);

    let cache_interest_interval =
        load_env_interval("ZO_CACHE_INTEREST_INTERVAL_MS", 5000);

    let update_funding_interval =
        load_env_interval("ZO_UPDATE_FUNDING_INTERVAL_MS", 30000);

    futures::join!(
        cache_oracle_loop(st, cache_oracle_interval),
        cache_interest_loop(st, cache_interest_interval, 4),
        update_funding_loop(st, update_funding_interval),
    );
}

fn load_env_interval(s: &str, default: u64) -> Interval {
    let ms = match env::var(s) {
        Ok(x) => u64::from_str(&x).expect(&format!("Failed to parse {}", s)),
        Err(_) => default,
    };
    let period = Duration::from_millis(ms);
    tokio::time::interval(period)
}

async fn cache_oracle_loop(st: &'static AppState, mut interval: Interval) {
    let (symbols, oracle_accounts): (Vec<String>, Vec<AccountMeta>) = st
        .zo_cache
        .oracles
        .iter()
        .filter(|x| !x.symbol.is_nil())
        .map(|x| {
            let symbol = String::try_from(&x.symbol).unwrap();
            let acc = AccountMeta::new_readonly(x.sources[0].key, false);
            (symbol, acc)
        })
        .unzip();

    let oracle_accounts: &'static _ = Box::leak(Box::new(oracle_accounts));

    loop {
        interval.tick().await;
        let symbols = symbols.clone();

        tokio::spawn(async move {
            let req = st
                .program
                .request()
                .args(zo_abi::instruction::CacheOracle {
                    symbols: symbols,
                    mock_prices: None,
                })
                .accounts(zo_abi::accounts::CacheOracle {
                    signer: st.program.payer(),
                    cache: st.zo_cache_pubkey,
                });

            let req = oracle_accounts
                .iter()
                .fold(req, |r, x| r.accounts(x.clone()));

            let _ = req.send().unwrap();
        });
    }
}

async fn cache_interest_loop(
    st: &'static AppState,
    mut interval: Interval,
    batch_size: usize,
) {
    let total = st.zo_state.total_collaterals as u8;

    loop {
        interval.tick().await;

        tokio::spawn(async move {
            let handles = (0..total).step_by(batch_size).map(|i| {
                let start = i;
                let end = min(i + (batch_size as u8), total);

                tokio::spawn(async move {
                    st.program
                        .request()
                        .args(zo_abi::instruction::CacheInterestRates {
                            start,
                            end,
                        })
                        .accounts(zo_abi::accounts::CacheInterestRates {
                            signer: st.program.payer(),
                            state: st.zo_state_pubkey,
                            cache: st.zo_cache_pubkey,
                        })
                        .send()
                })
            });

            let _ = futures::future::join_all(handles).await;
        });
    }
}

async fn update_funding_loop(st: &'static AppState, mut interval: Interval) {
    let markets: Vec<_> = st
        .zo_state
        .perp_markets
        .iter()
        .filter(|market| market.dex_market != Pubkey::default())
        .map(|market| {
            zo_abi::dex::ZoDexMarket::deserialize(
                market.dex_market,
                &st.rpc.get_account_data(&market.dex_market).unwrap(),
            )
        })
        .collect();

    let markets: &'static _ = Box::leak(Box::new(markets));

    loop {
        interval.tick().await;

        tokio::spawn(async move {
            let handles = markets.iter().cloned().map(|market| {
                tokio::spawn(async move {
                    st.program
                        .request()
                        .args(zo_abi::instruction::UpdatePerpFunding {})
                        .accounts(zo_abi::accounts::UpdatePerpFunding {
                            state: st.zo_state_pubkey,
                            state_signer: st.zo_state_signer_pubkey,
                            cache: st.zo_cache_pubkey,
                            dex_market: market.own_address,
                            market_bids: market.bids,
                            market_asks: market.asks,
                            dex_program: zo_abi::dex::ID,
                        })
                        .send()
                })
            });

            let _ = futures::future::join_all(handles).await;
        });
    }
}
