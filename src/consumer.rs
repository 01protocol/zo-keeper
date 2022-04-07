use crate::{error::Error, AppState};
use anchor_client::{
    anchor_lang::prelude::AccountMeta,
    solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey},
};
use std::{
    collections::{BTreeSet, HashMap},
    time::{Duration, Instant},
};
use tracing::{debug, info, trace, warn};

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
    let handles = st.load_dex_markets()?.into_iter().map(|(symbol, mkt)| {
        let cfg = cfg.clone();

        tokio::task::spawn_blocking(move || {
            let mut last_cranked_at = Instant::now() - cfg.max_wait;
            let mut accounts_table = HashMap::new();

            // The seq_num wraps at 1 << 32, so for the initial
            // value pick a number larger than that.
            let mut last_head = 1u64 << 48;

            loop {
                std::thread::sleep(Duration::from_millis(250));
                consume(
                    st,
                    &symbol,
                    &mkt,
                    &cfg,
                    &mut last_head,
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
    last_head: &mut u64,
    last_cranked_at: &mut Instant,
    // Control -> (Open Orders, Margin)
    accounts_table: &mut HashMap<Pubkey, (Pubkey, Pubkey)>,
) {
    let t = Instant::now();

    let (event_q_buf, slot) = {
        let res = st.rpc.get_account_with_commitment(
            &market.event_q,
            CommitmentConfig::confirmed(),
        );

        let res = match res {
            Ok(x) => x,
            Err(e) => {
                let e = Error::from(e);
                warn!("{}", e);
                return;
            }
        };

        let slot = res.context.slot;
        let buf = res.value.unwrap().data;

        (buf, slot)
    };

    tracing::Span::current().record("slot", &slot);

    let (events_header, events) =
        zo_abi::dex::Event::deserialize_queue(&event_q_buf).unwrap();
    let events = events.cloned().collect::<Vec<_>>();

    if events.is_empty() {
        trace!("no events, skipping");
        return;
    }

    if last_cranked_at.elapsed() < cfg.max_wait {
        if events_header.head == *last_head {
            debug!(
                "last cranked {}s ago and queue head still at {}, skipping",
                last_cranked_at.elapsed().as_secs(),
                { events_header.head },
            );
            return;
        }

        if events.len() < cfg.max_queue_length {
            debug!(
                "last cranked {}s ago and queue has {} events, skipping",
                last_cranked_at.elapsed().as_secs(),
                events.len(),
            );
            return;
        }
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
                    &st.program().account(control).unwrap(),
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
        t.elapsed().as_millis()
    );

    let market = *market;
    let limit = cfg.to_consume as u16;
    let span = tracing::Span::current();

    std::thread::spawn(move || {
        let _g = span.enter();
        consume_events(st, &market, limit, &control_accounts, &orders_accounts);

        let mid = control_accounts.len() / 2;
        let controls = control_accounts.split_at(mid);
        let orders = orders_accounts.split_at(mid);
        let margins = margin_accounts.split_at(mid);

        crank_pnl(st, &market, &controls.0, &orders.0, &margins.0);
        crank_pnl(st, &market, &controls.1, &orders.1, &margins.1);
    });

    *last_head = events_header.head;
    *last_cranked_at = Instant::now();
}

fn open_orders_pda(control: &Pubkey, zo_dex_market: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[control.as_ref(), zo_dex_market.as_ref()],
        &zo_abi::ZO_DEX_PID,
    )
    .0
}

fn margin_pda(control: &zo_abi::Control, state: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[control.authority.as_ref(), state.as_ref(), b"marginv1"],
        &zo_abi::ID,
    )
    .0
}

fn consume_events(
    st: &AppState,
    market: &zo_abi::dex::ZoDexMarket,
    limit: u16,
    control_accounts: &[AccountMeta],
    orders_accounts: &[AccountMeta],
) {
    let program = st.program();
    let req = program
        .request()
        .args(zo_abi::instruction::ConsumeEvents { limit })
        .accounts(zo_abi::accounts::ConsumeEvents {
            state: st.zo_state_pubkey,
            state_signer: st.zo_state_signer_pubkey,
            dex_program: zo_abi::ZO_DEX_PID,
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
            warn!("consume_events: {}", e);
        }
    }
}

fn crank_pnl(
    st: &AppState,
    market: &zo_abi::dex::ZoDexMarket,
    control_accounts: &[AccountMeta],
    orders_accounts: &[AccountMeta],
    margin_accounts: &[AccountMeta],
) {
    let program = st.program();
    let req = program
        .request()
        .args(zo_abi::instruction::CrankPnl)
        .accounts(zo_abi::accounts::CrankPnl {
            state: st.zo_state_pubkey,
            state_signer: st.zo_state_signer_pubkey,
            cache: st.zo_cache_pubkey,
            dex_program: zo_abi::ZO_DEX_PID,
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
            warn!("crank_pnl: {}", e);
        }
    }
}
