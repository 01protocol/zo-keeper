use crate::{error::Error, AppState};
use anchor_client::solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    instruction::{AccountMeta, Instruction},
};
use std::{marker::Send, sync::Arc, time::Duration};
use tokio::time::{Interval, MissedTickBehavior};
use tracing::{info, warn};

pub struct CrankConfig {
    pub cache_oracle_interval: Duration,
    pub cache_interest_interval: Duration,
    pub update_funding_interval: Duration,
}

const CACHE_ORACLE_CHUNK_SIZE: usize = 28;
const CACHE_ORACLE_CU_PER_ACCOUNT: usize = 1_400_000 / CACHE_ORACLE_CHUNK_SIZE;
const CACHE_INTEREST_CU_PER_ACCOUNT: usize = 30_000;
const UPDATE_FUNDING_CHUNK_SIZE: usize = 4;
const UPDATE_FUNDING_CU_PER_ACCOUNT: usize = 100_000;

pub async fn run(st: &'static AppState, cfg: CrankConfig) -> Result<(), Error> {
    let cache_oracle_tasks = st
        .iter_oracles()
        .filter(|x| String::from(x.symbol) != "LUNA")
        .collect::<Vec<_>>()
        .chunks(CACHE_ORACLE_CHUNK_SIZE)
        .map(|x| {
            let symbols: Vec<_> = x.iter().map(|o| o.symbol.into()).collect();
            let accounts: Vec<_> = x
                .iter()
                .map(|o| o.sources[0].key)
                .chain(
                    st.zo_state
                        .perp_markets
                        .iter()
                        .filter(|m| {
                            x.iter().any(|o| o.symbol == m.oracle_symbol)
                        })
                        .map(|m| m.dex_market),
                )
                .map(|k| AccountMeta::new_readonly(k, false))
                .collect();

            let symbols = Arc::new(symbols);
            let accounts = Arc::new(accounts);

            loop_blocking(interval(cfg.cache_oracle_interval), move || {
                cache_oracle(st, &symbols, &accounts)
            })
        })
        .collect::<Vec<_>>();

    let cache_interest_task =
        loop_blocking(interval(cfg.cache_interest_interval), move || {
            cache_interest(st)
        });

    let update_funding_tasks = st
        .load_dex_markets()?
        .into_iter()
        .filter(|(s, _)| s != "LUNA-PERP")
        .collect::<Vec<_>>()
        .chunks(UPDATE_FUNDING_CHUNK_SIZE)
        .map(|v| {
            let (s, m): (Vec<_>, Vec<_>) = v.iter().cloned().unzip();
            let symbols = Arc::new(s);
            let markets = Arc::new(m);

            loop_blocking(interval(cfg.update_funding_interval), move || {
                update_funding(st, &symbols, &markets)
            })
        })
        .collect::<Vec<_>>();

    futures::join!(
        futures::future::join_all(cache_oracle_tasks),
        cache_interest_task,
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
        .instruction(ComputeBudgetInstruction::request_units(
            (s.len() * CACHE_ORACLE_CU_PER_ACCOUNT) as u32,
            0,
        ))
        .args(zo_abi::instruction::CacheOracle {
            symbols: s.to_owned(),
            mock_prices: None,
        })
        .accounts(zo_abi::accounts::CacheOracle {
            signer: st.payer(),
            state: st.zo_state_pubkey,
            cache: st.zo_cache_pubkey,
            dex_program: zo_abi::ZO_DEX_PID,
        });

    let req = accs.iter().fold(req, |r, x| r.accounts(x.clone()));

    dispatch(st, req);
}

#[tracing::instrument(skip_all, level = "error")]
fn cache_interest(st: &AppState) {
    dispatch(
        st,
        st.program()
            .request()
            .instruction(ComputeBudgetInstruction::request_units(
                st.zo_state.total_collaterals as u32
                    * CACHE_INTEREST_CU_PER_ACCOUNT as u32,
                0,
            ))
            .args(zo_abi::instruction::CacheInterestRates {
                start: 0,
                end: st.zo_state.total_collaterals as u8,
            })
            .accounts(zo_abi::accounts::CacheInterestRates {
                signer: st.payer(),
                state: st.zo_state_pubkey,
                cache: st.zo_cache_pubkey,
            }),
    );
}

#[tracing::instrument(skip_all, level = "error", fields(symbol = ?symbol))]
fn update_funding(
    st: &AppState,
    symbol: &[String],
    m: &[zo_abi::dex::ZoDexMarket],
) {
    use anchor_lang::{InstructionData, ToAccountMetas};

    let program = st.program();
    let req =
        program
            .request()
            .instruction(ComputeBudgetInstruction::request_units(
                st.zo_state.total_collaterals as u32
                    * UPDATE_FUNDING_CU_PER_ACCOUNT as u32,
                0,
            ));

    let req = m.iter().fold(req, |acc, m| {
        acc.instruction(Instruction {
            program_id: zo_abi::ID,
            accounts: zo_abi::accounts::UpdatePerpFunding {
                state: st.zo_state_pubkey,
                state_signer: st.zo_state_signer_pubkey,
                cache: st.zo_cache_pubkey,
                dex_market: m.own_address,
                market_bids: m.bids,
                market_asks: m.asks,
                dex_program: zo_abi::ZO_DEX_PID,
            }
            .to_account_metas(None),
            data: zo_abi::instruction::UpdatePerpFunding {}.data(),
        })
    });

    dispatch(st, req);
}
