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

const CACHE_ORACLE_CHUNK_SIZE: usize = 6;
const CACHE_INTEREST_CHUNK_SIZE: usize = 12;

pub async fn run(st: &'static AppState, cfg: CrankConfig) -> Result<(), Error> {
    let cache_oracle_tasks = st
        .iter_oracles()
        .collect::<Vec<_>>()
        .chunks(CACHE_ORACLE_CHUNK_SIZE)
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
        .step_by(CACHE_INTEREST_CHUNK_SIZE)
        .map(|i| {
            let start = i;
            let end = min(
                i + CACHE_INTEREST_CHUNK_SIZE as u8,
                st.zo_state.total_collaterals as u8,
            );

            loop_blocking(interval(cfg.cache_interest_interval), move || {
                cache_interest(st, start, end)
            })
        });

    let update_funding_tasks =
        st.load_dex_markets()?.into_iter().map(|(symbol, market)| {
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

fn dispatch(st: &AppState, req: anchor_client::RequestBuilder) {
    use anchor_client::solana_sdk::{
        commitment_config::CommitmentConfig, signer::Signer as _,
        transaction::Transaction,
    };

    const GET_STATUS_RETRIES: usize = 25;
    const GET_STATUS_WAIT: u64 = 2000;

    // This auxiliary function emulates the same logic as the solana
    // client's `send_and_confirm_transaction` function, but does not
    // retry `usize::MAX` times as that ends up spawning too many
    // processes.
    let aux = move || -> Result<_, Error> {
        let ixs = req.instructions().unwrap();
        let bh = st.rpc.get_latest_blockhash()?;
        let payer = st.payer_key();
        let tx = Transaction::new_signed_with_payer(
            &ixs,
            Some(&payer.pubkey()),
            // NOTE: For cranking, no other signer is required.
            &[payer],
            bh,
        );
        let sg = st.rpc.send_transaction(&tx)?;

        for _ in 0..GET_STATUS_RETRIES {
            match st.rpc.get_signature_status(&sg)? {
                Some(Ok(_)) => return Ok(sg),
                Some(Err(e)) => return Err(e.into()),
                None => {
                    if !st.rpc.is_blockhash_valid(
                        &bh,
                        CommitmentConfig::processed(),
                    )? {
                        break;
                    }

                    std::thread::sleep(Duration::from_millis(GET_STATUS_WAIT));
                }
            }
        }

        Err(Error::ConfirmationTimeout(sg))
    };

    match aux() {
        Ok(sg) => info!("{}", sg),
        Err(e) => warn!("{}", e),
    };
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

    dispatch(st, req);
}

#[tracing::instrument(skip_all, level = "error", fields(from = start, to = end))]
fn cache_interest(st: &AppState, start: u8, end: u8) {
    dispatch(
        st,
        st.program()
            .request()
            .args(zo_abi::instruction::CacheInterestRates { start, end })
            .accounts(zo_abi::accounts::CacheInterestRates {
                signer: st.payer(),
                state: st.zo_state_pubkey,
                cache: st.zo_cache_pubkey,
            }),
    );
}

#[tracing::instrument(skip_all, level = "error", fields(symbol = symbol))]
fn update_funding(st: &AppState, symbol: &str, m: &zo_abi::dex::ZoDexMarket) {
    dispatch(
        st,
        st.program()
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
            }),
    );
}
