use crate::{error::Error, AppState};
use anchor_client::{
    anchor_lang::prelude::AccountMeta,
    solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey},
};
use std::{
    collections::{BTreeSet, HashMap},
    time::{Duration, Instant},
};
use tracing::{debug, info, warn};

#[derive(Clone)]
pub struct ConsumerConfig {
    pub to_consume: usize,
    pub max_wait: Duration,
    pub max_queue_length: usize,
}

pub async fn run(
    st: &'static AppState,
    cfg: ConsumerConfig,
) -> Result<(), Error> {
    let handles = st.load_dex_markets().map(|(symbol, mkt)| {
        let cfg = cfg.clone();

        tokio::task::spawn_blocking(move || {
            let mut max_slot_height = 0;
            let mut last_cranked_at = Instant::now() - cfg.max_wait;
            let mut accounts_table = HashMap::new();

            loop {
                std::thread::sleep(Duration::from_secs(1));
                consume(
                    st,
                    &symbol,
                    &mkt,
                    &cfg,
                    &mut max_slot_height,
                    &mut last_cranked_at,
                    &mut accounts_table,
                );
            }
        })
    });

    let _ = futures::future::join_all(handles).await;
    Ok(())
}

#[tracing::instrument(
    skip_all,
    level = "error",
    fields(symbol = symbol, slot = tracing::field::Empty)
)]
fn consume(
    st: &'static AppState,
    symbol: &str,
    market: &zo_abi::dex::ZoDexMarket,
    cfg: &ConsumerConfig,
    max_slot_height: &mut u64,
    last_cranked_at: &mut Instant,
    // Control -> (Open Orders, Margin)
    accounts_table: &mut HashMap<Pubkey, (Pubkey, Pubkey)>,
) {
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

    tracing::Span::current().record("slot", &slot);

    if slot <= *max_slot_height {
        info!(
            "already cranked for slot, skipping (max_seen_slot = {})",
            max_slot_height,
        );
        return;
    }

    let events = zo_abi::dex::Event::deserialize_queue(&event_q_buf)
        .unwrap()
        .1
        .cloned()
        .collect::<Vec<_>>();

    if events.is_empty() {
        debug!("no events, skipping");
        return;
    }

    if last_cranked_at.elapsed() < cfg.max_wait
        && events.len() < cfg.max_queue_length
    {
        info!(
            "last cranked {}s ago and queue only has {} events, skipping",
            last_cranked_at.elapsed().as_secs(),
            events.len(),
        );
        return;
    }

    // Sorted, unique, and capped list of control pubkeys.
    // Pubkeys are sorted by their [u64; 4] representation.
    let mut used_control: BTreeSet<[u64; 4]> = BTreeSet::new();

    for control in events.iter().map(|e| bytemuck::cast(e.control)) {
        used_control.insert(control);
        if used_control.len() >= cfg.to_consume {
            break;
        }
    }

    let mut control_accounts = Vec::with_capacity(used_control.len());
    let mut orders_accounts = Vec::with_capacity(used_control.len());
    let mut margin_accounts = Vec::with_capacity(used_control.len());

    for control in used_control.into_iter().map(bytemuck::cast) {
        let (oo, margin) = accounts_table.entry(control).or_insert_with(|| {
            (
                open_orders_pda(&control, &market.own_address),
                margin_pda(
                    &st.program.account(control).unwrap(),
                    &st.zo_state_pubkey,
                ),
            )
        });

        control_accounts.push(AccountMeta::new(control, false));
        orders_accounts.push(AccountMeta::new(*oo, false));
        margin_accounts.push(AccountMeta::new(*margin, false));
    }

    info!(
        "fetching {} events and {} unique orders took {}ms",
        events.len(),
        orders_accounts.len(),
        Instant::now().duration_since(t).as_millis()
    );

    {
        let req = st
            .program
            .request()
            .args(zo_abi::instruction::ConsumeEvents {
                limit: cfg.to_consume as u16,
            })
            .accounts(zo_abi::accounts::ConsumeEvents {
                state: st.zo_state_pubkey,
                state_signer: st.zo_state_signer_pubkey,
                dex_program: zo_abi::dex::ID,
                market: market.own_address,
                event_queue: market.event_q,
            });

        let res = control_accounts
            .iter()
            .chain(orders_accounts.iter())
            .fold(req, |r, x| r.accounts(x.clone()))
            .send();

        match res {
            Ok(sg) => info!("consume_events: {}", sg),
            Err(e) => {
                let e = Error::from(e);
                warn!("{}", e);
                return;
            }
        }
    };

    {
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

        let res = control_accounts
            .iter()
            .chain(orders_accounts.iter())
            .chain(margin_accounts.iter())
            .fold(req, |r, x| r.accounts(x.clone()))
            .send();

        match res {
            Ok(sg) => info!("crank_pnl: {}", sg),
            Err(e) => {
                let e = Error::from(e);
                warn!("{}", e);
                return;
            }
        }
    };

    *max_slot_height = slot;

    info!(
        "loop took {}ms",
        Instant::now().duration_since(t).as_millis()
    );
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
