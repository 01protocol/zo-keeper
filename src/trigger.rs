use crate::{error::Error, AppState};
use anchor_client::{
    anchor_lang::Discriminator,
    solana_client::{
        pubsub_client::PubsubClient,
        rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    },
    solana_sdk::{
        commitment_config::CommitmentConfig, pubkey::Pubkey,
        signature::Signature, sysvar,
    },
};
use parking_lot::{Mutex, RwLock, RwLockUpgradableReadGuard};
use solana_account_decoder::{UiAccountData, UiAccountEncoding};
use std::{collections::HashMap, str::FromStr};
use zo_abi as zo;

struct Accounts {
    pub zo_cache: Mutex<Option<zo::Cache>>,
    pub zo_so: RwLock<HashMap<Pubkey, RwLock<zo::SpecialOrders>>>,
    /// Mapping from authority key to (margin key, control key, control account).
    pub zo_trader_accs: RwLock<HashMap<Pubkey, (Pubkey, Pubkey, zo::Control)>>,
}

#[tracing::instrument(skip_all, name = "trigger", level = "error")]
pub fn run(st: &'static AppState) -> Result<(), Error> {
    let accs = Accounts {
        zo_cache: Mutex::new(Some(st.zo_cache)),
        zo_so: RwLock::new(
            st.program()
                .accounts::<zo::SpecialOrders>(vec![])?
                .into_iter()
                .map(|(k, v)| (k, RwLock::new(v)))
                .collect(),
        ),
        zo_trader_accs: Default::default(),
    };

    let mkts: HashMap<_, _> = st
        .load_dex_markets()?
        .into_iter()
        .map(|(_, m)| (m.own_address, m))
        .collect();

    std::thread::scope(|s| {
        s.spawn(|| listener(st, &accs));
        s.spawn(|| executer(st, &accs, mkts));
    });

    Ok(())
}

fn decode_ui_data(b: UiAccountData) -> Vec<u8> {
    match b {
        UiAccountData::Binary(b, _) => base64::decode(b).unwrap(),
        _ => unreachable!(),
    }
}

fn load_buf<T>(buf: &[u8]) -> Option<T>
where
    T: Copy + bytemuck::Pod + Discriminator,
{
    match buf[..8] == T::discriminator() {
        true => bytemuck::try_from_bytes(&buf[8..]).ok().copied(),
        false => None,
    }
}

fn margin_pda(authority: &Pubkey, state: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[authority.as_ref(), state.as_ref(), b"marginv1"],
        &zo::ID,
    )
    .0
}

#[tracing::instrument(skip_all, level = "error")]
fn listener(st: &'static AppState, accs: &Accounts) {
    loop {
        let r = PubsubClient::program_subscribe(
            st.cluster.ws_url(),
            &zo::ID,
            Some(RpcProgramAccountsConfig {
                filters: None,
                with_context: Some(false),
                account_config: RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    data_slice: None,
                    commitment: Some(CommitmentConfig::confirmed()),
                    min_context_slot: None,
                },
            }),
        );

        let (_sub, rx) = match r {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!("failed to connect: {}", Error::from(e));
                continue;
            }
        };

        while let Ok(r) = rx.recv() {
            let buf = decode_ui_data(r.value.account.data);

            if let Some(c) = load_buf::<zo::Cache>(&buf) {
                tracing::trace!("cache update");
                *accs.zo_cache.lock() = Some(c);
                continue;
            }

            if let Some(c) = load_buf::<zo::SpecialOrders>(&buf) {
                tracing::debug!("special orders update: {}", r.value.pubkey);

                let key = Pubkey::from_str(&r.value.pubkey).unwrap();
                let so = accs.zo_so.upgradable_read();

                match so.get(&key) {
                    Some(v) => *v.write() = c,
                    None => {
                        tracing::debug!("found new entry, inserting: {}", key);
                        let mut so = RwLockUpgradableReadGuard::upgrade(so);
                        so.insert(key, RwLock::new(c));
                    }
                }

                continue;
            }
        }

        tracing::warn!("disconnected, reconnecting...");
    }
}

#[tracing::instrument(skip_all, level = "error")]
fn executer(
    st: &'static AppState,
    accs: &Accounts,
    mut mkts: HashMap<Pubkey, zo::dex::ZoDexMarket>,
) {
    // Mapping from market key to index and dex market. Used for rapid lookups
    // when checking price, and for getting market addresses.
    let ms: HashMap<Pubkey, (usize, zo::dex::ZoDexMarket)> = st
        .zo_state
        .perp_markets
        .iter()
        .take_while(|m| m.dex_market != Pubkey::default())
        .enumerate()
        .map(|(i, m)| (m.dex_market, (i, mkts.remove(&m.dex_market).unwrap())))
        .collect();

    loop {
        let cache = match accs.zo_cache.lock().take() {
            Some(x) => x,
            None => {
                std::thread::sleep(std::time::Duration::from_millis(50));
                continue;
            }
        };

        // Get mark prices in small / big, mapped to index.
        let prices: Vec<u64> = cache
            .marks
            .iter()
            .zip(st.iter_markets())
            .map(|(c, m)| {
                use fixed::types::I80F48;
                (I80F48::from(c.price)
                    * I80F48::from_num(10u64.pow(m.asset_decimals.into())))
                .to_num()
            })
            .collect();

        std::thread::scope(|s| {
            for (k, so) in accs.zo_so.read().iter() {
                let so = so.read();
                for o in so.iter() {
                    if o.is_triggered(prices[ms[&o.market].0]) {
                        let (idx, mkt) = ms[&o.market];
                        let authority = { so.authority };
                        let k = *k;
                        let o = *o;

                        s.spawn(move || {
                            trigger(st, accs, &mkt, idx, authority, k, o)
                        });
                    }
                }
            }
        });
    }
}

#[tracing::instrument(
    skip_all,
    level = "error",
    fields(
        authority = %authority,
        market = %st.zo_state.perp_markets[idx].symbol,
        id = %{ order.id },
    ),
)]
fn trigger(
    st: &AppState,
    accs: &Accounts,
    mkt: &zo::dex::ZoDexMarket,
    idx: usize,
    authority: Pubkey,
    special_orders: Pubkey,
    order: zo::SpecialOrdersInfo,
) {
    match trigger_(st, accs, mkt, idx, authority, special_orders, order) {
        Ok(sg) => tracing::info!("{}", sg),
        Err(e) => tracing::warn!("{}", e),
    }
}

fn trigger_(
    st: &AppState,
    accs: &Accounts,
    mkt: &zo::dex::ZoDexMarket,
    idx: usize,
    authority: Pubkey,
    special_orders: Pubkey,
    order: zo::SpecialOrdersInfo,
) -> Result<Signature, Error> {
    tracing::debug!("triggering");
    let program = st.program();

    // Get the margin and control keys. If they don't
    // exist, fetch them first, and update the cache.
    let (margin, control, oo) = {
        let map = accs.zo_trader_accs.read();

        match map.get(&authority).copied() {
            Some((margin_key, control_key, control)) => {
                let oo_key = control.open_orders_agg[idx].key;

                // Since the other branch drops this, drop it here
                // too so it doesn't get held for too long
                drop(map);

                (margin_key, control_key, oo_key)
            }
            None => {
                // First, drop the readlock, since we have to insert
                // a new entry.
                drop(map);

                let margin_key = margin_pda(&authority, &st.zo_state_pubkey);
                let margin = program.account::<zo::Margin>(margin_key)?;
                let control_key = margin.control;
                let control = program.account::<zo::Control>(control_key)?;
                let oo_key = control.open_orders_agg[idx].key;

                accs.zo_trader_accs
                    .write()
                    .insert(authority, (margin_key, control_key, control));

                (margin_key, control_key, oo_key)
            }
        }
    };

    program
        .request()
        .args(zo::instruction::ExecuteSpecialOrder { id: order.id })
        .accounts(zo::accounts::ExecuteSpecialOrder {
            state: st.zo_state_pubkey,
            state_signer: st.zo_state_signer_pubkey,
            cache: st.zo_cache_pubkey,
            payer: st.payer(),
            authority,
            margin,
            control,
            special_orders,
            open_orders: oo,
            dex_market: mkt.own_address,
            req_q: mkt.req_q,
            event_q: mkt.event_q,
            market_bids: mkt.bids,
            market_asks: mkt.asks,
            dex_program: zo::ZO_DEX_PID,
            rent: sysvar::rent::ID,
        })
        .send()
        .map_err(Into::into)
}
