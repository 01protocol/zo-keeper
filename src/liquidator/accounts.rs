/*
 * This files contains the data structure responsible
 * for maintaining a hierarchy of control accounts.
 * Each account is quite big, ~7kB, so they
 * need to be compressed to save space, then properly updated when need be.
 *
 * Let's start by storing everything to make sure the logic is good,
 * then deal with compression.
*/
use crate::liquidator::{
    error::ErrorCode, liquidation, margin_utils::*, utils::*,
};

use fixed::types::I80F48;
use serum_dex::state::{
    Market as SerumMarket, MarketState as SerumMarketState,
};
use solana_sdk::pubkey::Pubkey;
use std::{
    collections::HashMap,
    ops::Deref,
    sync::{Arc, Mutex, MutexGuard},
};

use tracing::{error, error_span, info};
use zo_abi::{
    dex::ZoDexMarket as MarketState, Cache, Control, FractionType, Margin,
    State, MAX_MARKETS,
};

// Let's start with a simple hashtable
// It has to be sharable.
pub struct AccountTable {
    // Table for margin accounts
    margin_table: HashMap<Pubkey, Margin>,

    // The control accounts table
    control_table: HashMap<Pubkey, Control>,

    // The cache account
    cache: Cache,
    cache_key: Pubkey,

    // The state account
    state: State,
    state_key: Pubkey,
    state_signer: Pubkey,

    // The market state accounts
    market_state: Vec<MarketState>,

    // The serum markets for swapping
    serum_markets: HashMap<usize, SerumMarketState>,
    serum_vault_signers: HashMap<usize, Pubkey>,

    payer_key: Pubkey,
    payer_margin_key: Pubkey,
    payer_margin: Margin,
    payer_control_key: Pubkey,
    payer_control: Control,

    worker_count: u8,
    worker_index: u8,
}

impl AccountTable {
    pub fn new(
        st: &crate::AppState,
        worker_index: u8,
        worker_count: u8,
    ) -> Result<Self, crate::Error> {
        // This fetches all on-chain accounts for a start
        // Assumes that the dex is started, i.e. there's a cache
        // Also need to load market state info.

        let payer = st.payer();
        let payer_margin_key = Pubkey::find_program_address(
            &[payer.as_ref(), st.zo_state_pubkey.as_ref(), b"marginv1"],
            &zo_abi::ID,
        )
        .0;
        let payer_margin = get_type_from_account::<Margin>(
            &payer_margin_key,
            &mut st
                .rpc
                .get_account(&payer_margin_key)
                .expect("Could not get payer margin account"),
        );
        let payer_control_key = payer_margin.control;
        let payer_control = get_type_from_account::<Control>(
            &payer_control_key,
            &mut st.rpc.get_account(&payer_control_key).unwrap(),
        );

        let margin_table: HashMap<_, _> =
            load_program_accounts::<Margin>(&st.rpc, &zo_abi::ID)?
                .into_iter()
                .filter(|(_, a)| {
                    is_right_remainder(&a.control, worker_count, worker_index)
                })
                .collect();

        let control_table: HashMap<_, _> =
            load_program_accounts::<Control>(&st.rpc, &zo_abi::ID)?
                .into_iter()
                .filter(|(k, _)| {
                    is_right_remainder(&k, worker_count, worker_index)
                })
                .collect();

        let market_state: Vec<_> =
            st.load_dex_markets()?.into_iter().map(|(_, m)| m).collect();

        let mut serum_markets: HashMap<usize, _> = HashMap::new();
        let mut serum_vault_signers: HashMap<usize, _> = HashMap::new();

        for (i, collateral_info) in st.iter_collaterals().enumerate() {
            if !collateral_info.is_swappable {
                continue;
            }

            let serum_oo_account = st
                .rpc
                .get_account(&collateral_info.serum_open_orders)
                .unwrap();

            let serum_market_address =
                Pubkey::new(&serum_oo_account.data[13..45]);
            let mut serum_market_account =
                st.rpc.get_account(&serum_market_address).unwrap();
            let serum_market_account_info = get_account_info(
                &serum_market_address,
                &mut serum_market_account,
            );

            let market_state = SerumMarket::load(
                &serum_market_account_info,
                &zo_abi::SERUM_DEX_PID,
                true,
            )
            .unwrap();
            let market = market_state.deref();

            serum_markets.insert(i, *market);

            let vault_signer = Pubkey::create_program_address(
                &[
                    array_to_pubkey(&{ market.own_address }).as_ref(),
                    &market.vault_signer_nonce.to_le_bytes(),
                ],
                &zo_abi::SERUM_DEX_PID,
            )
            .unwrap();

            serum_vault_signers.insert(i, vault_signer);
        }

        Ok(Self {
            margin_table,
            control_table,
            cache: st.zo_cache,
            cache_key: st.zo_cache_pubkey,
            state: st.zo_state,
            state_key: st.zo_state_pubkey,
            state_signer: st.zo_state_signer_pubkey,
            market_state,
            serum_markets,
            serum_vault_signers,
            payer_key: payer,
            payer_margin_key,
            payer_margin,
            payer_control_key,
            payer_control,
            worker_count,
            worker_index,
        })
    }

    pub fn refresh_accounts(
        &mut self,
        st: &crate::AppState,
    ) -> Result<(), crate::Error> {
        *self = Self::new(st, self.worker_index, self.worker_count)?;
        Ok(())
    }

    pub fn update_margin(&mut self, key: Pubkey, account: Margin) {
        if is_right_remainder(
            &account.control,
            self.worker_count,
            self.worker_index,
        ) {
            self.margin_table.insert(key, account);
        }
    }

    pub fn update_control(&mut self, key: Pubkey, account: Control) {
        if is_right_remainder(&key, self.worker_count, self.worker_index) {
            self.control_table.insert(key, account);
        }
    }

    pub fn update_cache(&mut self, cache: Cache) {
        self.cache = cache;
    }

    pub fn update_state(&mut self, state: State) {
        self.state = state;
    }

    /// The number of control accounts.
    pub fn size(&self) -> usize {
        self.control_table.len()
    }

    pub fn payer_key(&self) -> Pubkey {
        self.payer_key
    }

    pub fn payer_margin_key(&self) -> Pubkey {
        self.payer_margin_key
    }

    pub fn payer_margin(&self) -> &Margin {
        &self.payer_margin
    }

    pub fn payer_control_key(&self) -> Pubkey {
        self.payer_control_key
    }

    pub fn payer_control(&self) -> &Control {
        &self.payer_control
    }

    pub fn get_control_from_margin(
        &self,
        margin: &Margin,
    ) -> Option<(&Pubkey, &Control)> {
        self.control_table.get_key_value(&margin.control)
    }
}

pub type Db = Arc<Mutex<AccountTable>>;

#[derive(Clone)]
pub struct DbWrapper {
    db: Db,
}

impl DbWrapper {
    pub fn new(
        st: &crate::AppState,
        worker_index: u8,
        worker_count: u8,
    ) -> Self {
        DbWrapper {
            db: Arc::new(Mutex::new(
                AccountTable::new(st, worker_index, worker_count).unwrap(),
            )),
        }
    }

    pub async fn check_all_accounts(
        &self,
        st: &'static crate::AppState,
        dex_program: &Pubkey,
        serum_dex_program: &Pubkey,
    ) -> Result<usize, ErrorCode> {
        let (size, handles) =
            self.check_all_accounts_aux(st, dex_program, serum_dex_program)?;
        match futures::future::try_join_all(handles).await {
            Ok(_) => Ok(size),
            Err(_) => Err(ErrorCode::LiquidationFailure),
        }
    }

    pub fn check_all_accounts_aux(
        &self,
        st: &'static crate::AppState,
        dex_program: &Pubkey,
        serum_dex_program: &Pubkey,
    ) -> Result<(usize, Vec<tokio::task::JoinHandle<()>>), ErrorCode> {
        let db_clone = self.get_clone();
        let db: &mut MutexGuard<AccountTable> =
            &mut db_clone.lock().map_err(|_| ErrorCode::LockFailure)?;

        let mut handles: Vec<tokio::task::JoinHandle<_>> = Vec::new();
        let span = error_span!("check_all_accounts");
        for (key, margin) in db.margin_table.clone().into_iter() {
            let (cancel_orders, liquidate) =
                DbWrapper::is_liquidatable(&margin, &db, &db.state, &db.cache)?;
            if liquidate {
                span.in_scope(|| {
                    info!(
                        "Found liquidatable account: {}",
                        margin.authority.to_string()
                    )
                });
                // Get the updated payer accounts

                /*******************************/
                let dex_program = *dex_program;
                let serum_dex_program = *serum_dex_program;
                let payer_pubkey = db.payer_key();
                let payer_margin_key = db.payer_margin_key();
                let payer_margin = *db.payer_margin();
                let payer_control_key = db.payer_control_key();
                let payer_control = *db.payer_control();
                let payer_oo: [Pubkey; MAX_MARKETS as usize] =
                    get_oo_keys(&payer_control.open_orders_agg);
                let control_pair = db.get_control_from_margin(&margin).unwrap();
                let control = *control_pair.1;
                let cache = db.cache;
                let cache_key = db.cache_key;
                let state = db.state;
                let state_key = db.state_key;
                let state_signer = db.state_signer;
                let market_state = db.market_state.clone();
                let serum_markets = db.serum_markets.clone();
                let serum_vault_signers = db.serum_vault_signers.clone();

                // TODO: Refactor to have a struct for this, right now it's a mess
                let span_clone = span.clone();
                let handle = tokio::task::spawn_blocking(move || {
                    let result = liquidation::liquidate(
                        &st.program(),
                        &dex_program,
                        &payer_pubkey,
                        &payer_margin,
                        &payer_margin_key,
                        &payer_control,
                        &payer_control_key,
                        &payer_oo,
                        &key,
                        &margin,
                        &control,
                        &cache,
                        &cache_key,
                        &state,
                        &state_key,
                        &state_signer,
                        market_state.clone(),
                        serum_markets,
                        &serum_dex_program,
                        serum_vault_signers,
                    );

                    match result {
                        Ok(()) => {
                            span_clone.in_scope(|| {
                                info!("Liquidated {}", margin.authority);
                            });
                        }
                        Err(e) => {
                            span_clone.in_scope(|| {
                                error!(
                                    "{} not liquidated: {:?}",
                                    margin.authority, e
                                )
                            });
                        }
                    }
                });

                handles.push(handle);
            } else if cancel_orders {
                span.in_scope(|| {
                    info!(
                        "Found cancellable account: {}",
                        margin.authority.to_string()
                    )
                });
                let dex_program = *dex_program;
                let payer_pubkey = db.payer_key();
                let control_pair = db.get_control_from_margin(&margin).unwrap();
                let control = *control_pair.1;
                let cache = db.cache;
                let cache_key = db.cache_key;
                let state = db.state;
                let state_key = db.state_key;
                let state_signer = db.state_signer;
                let market_state = db.market_state.clone();

                let span_clone = span.clone();
                let handle = tokio::task::spawn_blocking(move || {
                    let result = liquidation::cancel(
                        &st.program(),
                        &dex_program,
                        &payer_pubkey,
                        &key,
                        &margin,
                        &control,
                        &cache,
                        &cache_key,
                        &state,
                        &state_key,
                        &state_signer,
                        market_state.clone(),
                    );

                    match result {
                        Ok(()) => (),
                        Err(e) => {
                            span_clone.in_scope(|| {
                                error!(
                                    "Error cancelling account {} : {:?}",
                                    margin.authority, e
                                )
                            });
                        }
                    }
                });
                handles.push(handle);
            }
        }

        Ok((db.size(), handles))
    }

    fn is_liquidatable(
        margin: &Margin,
        table: &AccountTable,
        state: &State,
        cache: &Cache,
    ) -> Result<(bool, bool), ErrorCode> {
        // Do the math on the margin account.
        // let span = error_span!("is_liquidatable");
        // let col = get_total_collateral(margin, cache, state);
        // println!("{}", margin.authority);
        let control = match table.get_control_from_margin(margin) {
            Some((_key, control)) => control,
            None => {
                // In this case, a margin account was just created with it's control, but the listener didn't catch the control.
                // I.e. This account is very low risk, so just skip checking this account.
                // It will be fetched the next time all accounts are fetched, i.e. in five minutes
                // TODO: Fetch the margin
                return Ok((false, false));
            }
        };
        let has_oo = has_open_orders(cache, control)?;

        let is_above_cancel = check_mf(
            FractionType::Cancel,
            margin,
            control,
            state,
            cache,
            I80F48::from_num(0.99995f64),
        );

        let is_above_maintenance = check_mf(
            FractionType::Maintenance,
            margin,
            control,
            state,
            cache,
            I80F48::from_num(0.99995f64),
        );

        Ok((!is_above_cancel && has_oo, !is_above_maintenance))
    }

    pub fn get_clone(&self) -> Db {
        self.db.clone()
    }

    pub fn get(&self) -> &Db {
        &self.db
    }

    pub fn refresh_accounts(
        &self,
        st: &crate::AppState,
    ) -> Result<(), crate::Error> {
        let mut db = self.db.lock().unwrap();
        db.refresh_accounts(st)?;
        Ok(())
    }
}
