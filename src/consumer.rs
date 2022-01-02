use crate::{error::Error, AppState};
use anchor_client::{
    anchor_lang::prelude::AccountMeta,
    solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey},
};
use std::{
    collections::{BTreeSet, HashMap},
    time::{Duration, Instant},
};
use tracing::{error_span, info};

pub async fn run(
    st: &'static AppState,
    to_consume: usize,
    max_wait: Duration,
    max_queue_length: usize,
) -> Result<(), Error> {
    let handles = st.load_dex_markets().map(|(symbol, mkt)| {
        tokio::spawn(consumer_loop(
            st,
            symbol.clone(),
            mkt,
            to_consume,
            max_wait,
            max_queue_length,
        ))
    });

    let _ = futures::future::join_all(handles).await;
    Ok(())
}

async fn consumer_loop(
    st: &'static AppState,
    symbol: String,
    market: zo_abi::dex::ZoDexMarket,
    to_consume: usize,
    max_wait: Duration,
    max_queue_length: usize,
) -> Result<(), Error> {
    let mut interval = tokio::time::interval(Duration::from_secs(1));

    let mut max_slot_height = 0u64;
    let last_cranked_at = Instant::now() - max_wait;

    // control -> (open orders, margin)
    let mut accounts_table: HashMap<Pubkey, (Pubkey, Pubkey)> = HashMap::new();

    loop {
        interval.tick().await;
        let t = Instant::now();

        let (event_q_buf, slot) = {
            let event_q_value_and_context = st
                .rpc
                .get_account_with_commitment(
                    &market.event_q,
                    CommitmentConfig::confirmed(),
                )
                .unwrap();

            let slot = event_q_value_and_context.context.slot;
            let buf = event_q_value_and_context.value.unwrap().data;

            (buf, slot)
        };

        let span = error_span!("consumer_loop", symbol = symbol.as_str(), slot);

        if slot <= max_slot_height {
            span.in_scope(|| {
                info!(
                    "already cranked for slot, skipping (max_seen_slot = {})",
                    max_slot_height,
                )
            });
            continue;
        }

        let events = zo_abi::dex::Event::deserialize_queue(&event_q_buf)
            .unwrap()
            .1
            .cloned()
            .collect::<Vec<_>>();

        if events.is_empty() {
            continue;
        }

        if last_cranked_at.elapsed() < max_wait
            && events.len() < max_queue_length
        {
            span.in_scope(|| {
                info!(
                    "last cranked {}s ago and queue only has {} events, skipping",
                    last_cranked_at.elapsed().as_secs(),
                    events.len(),
                )
            });
            continue;
        }

        span.in_scope(|| info!("event queue length: {}", events.len()));

        // Sorted, unique, and capped list of control pubkeys.
        let mut used_control = BTreeSet::new();

        for control in events.iter().map(|e| e.control) {
            used_control.insert(control);
            if used_control.len() >= to_consume {
                break;
            }
        }

        let mut control_accounts = Vec::with_capacity(used_control.len());
        let mut orders_accounts = Vec::with_capacity(used_control.len());
        let mut margin_accounts = Vec::with_capacity(used_control.len());

        for control in used_control.iter() {
            let (oo, margin) =
                accounts_table.entry(*control).or_insert_with(|| {
                    (
                        open_orders_pda(control, &market.own_address),
                        margin_pda(
                            &st.program.account(*control).unwrap(),
                            &st.zo_state_pubkey,
                        ),
                    )
                });

            control_accounts.push(AccountMeta::new(*control, false));
            orders_accounts.push(AccountMeta::new(*oo, false));
            margin_accounts.push(AccountMeta::new(*margin, false));
        }

        span.in_scope(|| {
            info!("number of unique order accounts: {}", orders_accounts.len())
        });

        span.in_scope(|| {
            info!(
                "fetching {} events from the queue took {}ms",
                events.len(),
                Instant::now().duration_since(t).as_millis()
            )
        });

        let crank_pnl_res = {
            let req = st
                .program
                .request()
                .args(zo_abi::instruction::CrankPnl)
                .accounts(zo_abi::accounts::CrankPnl {
                    state: st.zo_state_pubkey,
                    state_signer: st.zo_state_signer_pubkey,
                    cache: st.zo_cache_pubkey,
                    dex_program: zo_abi::dex::ID,
                    market: market.own_address,
                });

            let req = control_accounts
                .iter()
                .chain(orders_accounts.iter())
                .chain(margin_accounts.iter())
                .fold(req, |r, x| r.accounts(x.clone()));

            req.send()
        };

        match crank_pnl_res {
            Ok(sg) => span.in_scope(|| info!("crank_pnl: {}", sg)),
            Err(e) => {
                st.error(span.clone(), e).await;
                continue;
            }
        }

        let consume_events_res = {
            let req = st
                .program
                .request()
                .args(zo_abi::instruction::ConsumeEvents {
                    limit: to_consume as u16,
                })
                .accounts(zo_abi::accounts::ConsumeEvents {
                    state: st.zo_state_pubkey,
                    state_signer: st.zo_state_signer_pubkey,
                    dex_program: zo_abi::dex::ID,
                    market: market.own_address,
                    event_queue: market.event_q,
                });

            let req = control_accounts
                .iter()
                .chain(orders_accounts.iter())
                .fold(req, |r, x| r.accounts(x.clone()));

            req.send()
        };

        match consume_events_res {
            Ok(sg) => span.in_scope(|| info!("consume_events: {}", sg)),
            Err(e) => {
                st.error(span.clone(), e).await;
                continue;
            }
        }

        max_slot_height = slot;

        span.in_scope(|| {
            info!(
                "loop took {}ms",
                Instant::now().duration_since(t).as_millis()
            )
        });
    }
}

pub fn open_orders_pda(control: &Pubkey, zo_dex_market: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[control.as_ref(), zo_dex_market.as_ref()],
        &zo_abi::dex::ID,
    )
    .0
}

pub fn margin_pda(control: &zo_abi::Control, state: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[control.authority.as_ref(), state.as_ref(), b"marginv1"],
        &zo_abi::ID,
    )
    .0
}
