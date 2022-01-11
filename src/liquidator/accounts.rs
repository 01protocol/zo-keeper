/*
 * This files contains the data structure responsible
 * for maintaining a hierarchy of control accounts.
 * Each account is quite big, ~7kB, so they
 * need to be compressed to save space, then properly updated when need be.
 *
 * Let's start by storing everything to make sure the logic is good,
 * then deal with compression.
*/
use anchor_client::{Client, Program};

use fixed::types::I80F48;
use futures::future::join_all;

use serum_dex::state::{
    Market as SerumMarket, MarketState as SerumMarketState,
};

use solana_client::rpc_client::RpcClient;
use solana_program::pubkey::Pubkey as SolanaPubkey;
use solana_sdk::pubkey::Pubkey;

use std::{
    cell::RefCell,
    collections::{hash_map::IntoIter, HashMap},
    mem,
    ops::Deref,
    sync::{Arc, Mutex, MutexGuard},
};

use tokio::runtime::Runtime;

use tracing::{error, error_span, info};

use zo_abi::{
    dex::ZoDexMarket as MarketState, Cache, Control, FractionType, Margin,
    State, WrappedI80F48, MAX_MARKETS,
};

use crate::liquidator::{
    error::ErrorCode, liquidation, margin_utils::*, math::*, opts::Opts,
    utils::*,
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
    // The number of stored accounts
    // Only track number of controls
    size: u64,
}

impl AccountTable {
    pub fn new(options: &Opts, payer_pubkey: &Pubkey) -> Self {
        // This fetches all on-chain accounts for a start
        // Assumes that the dex is started, i.e. there's a cache
        // Also need to load market state info.

        let client = RpcClient::new(options.http_endpoint.clone());
        let mut size: u64 = 0;
        let mut margin_table = HashMap::new();
        let mut control_table = HashMap::new();

        for (key, mut margin) in get_accounts(
            &client,
            &options.zo_program,
            mem::size_of::<Margin>() as u64 + 8u64,
        )
        .unwrap()
        {
            let account = get_type_from_account::<Margin>(&key, &mut margin);
            if is_right_remainder(
                &account.control,
                &options.num_workers,
                &options.n,
            ) || account.authority.eq(payer_pubkey)
            {
                margin_table.insert(key, account);
            }
        }

        for (key, mut control) in get_accounts(
            &client,
            &options.zo_program,
            mem::size_of::<Control>() as u64 + 8u64,
        )
        .unwrap()
        {
            let account = get_type_from_account::<Control>(&key, &mut control);
            if is_right_remainder(&key, &options.num_workers, &options.n)
                || account.authority.eq(payer_pubkey)
            {
                control_table.insert(key, account);
                size += 1;
            }
        }
        let span =
            error_span!("account_table_new", "{}", payer_pubkey.to_string());

        span.in_scope(|| info!("State size: {}", mem::size_of::<State>()));
        span.in_scope(|| info!("Cache size: {}", mem::size_of::<Cache>()));
        span.in_scope(|| info!("Margin size: {}", mem::size_of::<Margin>()));
        span.in_scope(|| info!("Control size: {}", mem::size_of::<Control>()));

        let (cache, cache_key) = match get_accounts(
            &client,
            &options.zo_program,
            mem::size_of::<Cache>() as u64 + 8u64,
        ) {
            Ok(vec) => {
                if vec.is_empty() {
                    panic!("No cache accounts!");
                } else if vec.len() > 1 {
                    panic!("Too many cache accounts! {} found.", vec.len());
                }
                (
                    get_type_from_account::<Cache>(
                        &vec[0].0,
                        &mut vec[0].1.clone(),
                    ),
                    vec[0].0,
                )
            }
            Err(e) => {
                panic!("{:?}", e);
            }
        };

        let (state, state_key) = match get_accounts(
            &client,
            &options.zo_program,
            mem::size_of::<State>() as u64 + 8u64,
        ) {
            Ok(vec) => {
                if vec.is_empty() {
                    panic!("No state accounts!");
                } else if vec.len() > 1 {
                    panic!("Too many state accounts!");
                }
                (
                    get_type_from_account::<State>(
                        &vec[0].0,
                        &mut vec[0].1.clone(),
                    ),
                    vec[0].0,
                )
            }
            Err(e) => {
                panic!("{:?}", e);
            }
        };

        let (state_signer, _state_signer_nonce): (Pubkey, u8) =
            SolanaPubkey::find_program_address(
                &[&state_key.to_bytes()],
                &options.zo_program,
            );

        let mut market_state: Vec<MarketState> =
            Vec::with_capacity(state.perp_markets.len());

        for market_info in state.perp_markets {
            if market_info.dex_market == Pubkey::default() {
                continue;
            }
            let market_account =
                client.get_account(&market_info.dex_market).unwrap();

            market_state.push(
                *MarketState::deserialize(&market_account.data)
                    .unwrap()
                    .deref(),
            );
        }

        let mut serum_markets: HashMap<usize, SerumMarketState> =
            HashMap::with_capacity(state.collaterals.len());
        let mut serum_vault_signers: HashMap<usize, Pubkey> =
            HashMap::with_capacity(state.collaterals.len());

        for (i, collateral_info) in state.collaterals.iter().enumerate() {
            if collateral_info.mint == Pubkey::default()
                || !collateral_info.is_swappable
            {
                continue;
            }
            let serum_oo_account = client
                .get_account(&collateral_info.serum_open_orders)
                .unwrap();

            let serum_market_address =
                Pubkey::new(&serum_oo_account.data[13..45]);
            let mut serum_market_account =
                client.get_account(&serum_market_address).unwrap();
            let serum_market_account_info = get_account_info(
                &serum_market_address,
                &mut serum_market_account,
            );

            let market_state = SerumMarket::load(
                &serum_market_account_info,
                &options.serum_dex_program,
                true,
            )
            .unwrap();
            let market = market_state.deref();

            serum_markets.insert(i, *market);
            /*
            let key = Pubkey::new(cast_slice(&identity(market.own_address) as &[_]));
            let nonce = bytes_of({&market.vault_signer_nonce});
            let seeds = [key.as_ref(), nonce];

            let vault_signer = Pubkey::create_program_address(
                &seeds,
                &options.serum_dex_program,
            ).unwrap();
            */

            let vault_signer = Pubkey::create_program_address(
                &[
                    array_to_pubkey(&{ market.own_address }).as_ref(),
                    &market.vault_signer_nonce.to_le_bytes(),
                ],
                &options.serum_dex_program,
            )
            .unwrap();

            serum_vault_signers.insert(i, vault_signer);
        }

        AccountTable {
            margin_table,
            control_table,
            cache,
            cache_key,
            state,
            state_key,
            state_signer,
            market_state,
            serum_markets,
            serum_vault_signers,
            size,
        }
    }

    pub fn refresh_accounts(&mut self, opts: &Opts, payer_pubkey: &Pubkey) {
        let new_table = Self::new(opts, payer_pubkey);
        self.margin_table = new_table.margin_table;
        self.control_table = new_table.control_table;
        self.cache = new_table.cache;
        self.cache_key = new_table.cache_key;
        self.state = new_table.state;
        self.state_key = new_table.state_key;
        self.state_signer = new_table.state_signer;
        self.market_state = new_table.market_state;
        self.serum_markets = new_table.serum_markets;
        self.serum_vault_signers = new_table.serum_vault_signers;
        self.size = new_table.size;
    }

    pub fn update_margin(&mut self, key: &Pubkey, account: &Margin) {
        self.margin_table.insert(*key, *account);
    }

    pub fn update_control(&mut self, key: &Pubkey, account: &Control) {
        match self.control_table.insert(*key, *account) {
            Some(_) => (),
            None => self.size += 1,
        }
    }

    pub fn update_cache(&mut self, cache: &Cache) {
        self.cache = *cache;
    }

    pub fn update_state(&mut self, state: &State) {
        self.state = *state;
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn control_iterator(&self) -> IntoIter<Pubkey, Control> {
        let copy = self.control_table.clone();
        copy.into_iter()
    }

    pub fn margin_iterator(&self) -> IntoIter<Pubkey, Margin> {
        let copy = self.margin_table.clone();
        copy.into_iter()
    }

    pub fn get_control_from_margin(
        &self,
        margin: &Margin,
    ) -> Option<(&Pubkey, &Control)> {
        self.control_table.get_key_value(&margin.control)
    }
}

pub type Db = Arc<Mutex<AccountTable>>;

pub struct DbWrapper {
    db: Db,
}

impl DbWrapper {
    pub fn new(options: &Opts, payer_pubkey: &Pubkey) -> Self {
        DbWrapper {
            db: Arc::new(Mutex::new(AccountTable::new(options, payer_pubkey))),
        }
    }

    pub fn check_all_accounts(
        &self,
        runtime: &Runtime,
        anchor_client: &Client,
        program_id: &Pubkey,
        dex_program: &Pubkey,
        serum_dex_program: &Pubkey,
        payer_pubkey: &Pubkey,
        payer_margin_key: &Pubkey,
    ) -> Result<u64, ErrorCode> {
        let db_clone = self.get_clone();
        let db: &mut MutexGuard<AccountTable> =
            &mut db_clone.lock().map_err(|_| ErrorCode::LockFailure)?;

        let mut handles: Vec<tokio::task::JoinHandle<_>> = Vec::new();
        let span = error_span!("check_all_accounts");
        for (key, margin) in db.margin_table.clone().into_iter() {
            /*
             * Liquidation procedure:
             *   1. Check if mf < mmf while omittting open orders
             *   2. If so, cancel all their orders
             *   3. Then, find the spot or perp market with largest position
             *   4. Cancel that position
             * In other words, this performs only a single position on all accounts.
             */
            let (cancel_orders, liquidate) = DbWrapper::is_liquidatable(
                &margin,
                db,
                &db.state.clone(),
                &db.cache.clone(),
            )?;
            if liquidate {
                span.in_scope(|| {
                    info!(
                        "Found liquidatable account: {}",
                        margin.authority.to_string()
                    )
                });
                // Get the updated payer accounts

                /*******************************/
                let program: Program = anchor_client.program(*program_id);
                let dex_program = *dex_program;
                let serum_dex_program = *serum_dex_program;
                let payer_pubkey = *payer_pubkey;
                let payer_margin_key = *payer_margin_key;
                let payer_margin =
                    *db.margin_table.get(&payer_margin_key).unwrap();
                let (payer_control_key, payer_control) =
                    db.get_control_from_margin(&payer_margin).unwrap();
                let payer_control_key = *payer_control_key;
                let payer_control = *payer_control;
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
                let handle = runtime.spawn(async move {
                    let result = liquidation::liquidate(
                        &program,
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
                        Ok(()) => (),
                        Err(e) => {
                            span_clone.in_scope(|| {
                                error!(
                                    "Error liquidating account {} : {:?}",
                                    margin.authority, e
                                )
                            });
                        }
                    }
                });

                handles.push(handle);
            } else if cancel_orders {
                let program: Program = anchor_client.program(*program_id);
                let dex_program = *dex_program;
                let payer_pubkey = *payer_pubkey;
                let control_pair = db.get_control_from_margin(&margin).unwrap();
                let control = *control_pair.1;
                let cache = db.cache;
                let cache_key = db.cache_key;
                let state = db.state;
                let state_key = db.state_key;
                let state_signer = db.state_signer;
                let market_state = db.market_state.clone();

                let span_clone = span.clone();
                let handle = runtime.spawn(async move {
                    let result = liquidation::cancel(
                        &program,
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
                                    "Error liquidating account {} : {:?}",
                                    margin.authority, e
                                )
                            });
                        }
                    }
                });
                handles.push(handle);
            }
        }

        for result in runtime.block_on(join_all(handles)) {
            match result {
                Ok(()) => (),
                Err(_) => return Err(ErrorCode::LiquidationFailure),
            }
        }

        Ok(db.size())
    }

    fn is_liquidatable(
        margin: &Margin,
        table: &MutexGuard<AccountTable>,
        state: &State,
        cache: &Cache,
    ) -> Result<(bool, bool), ErrorCode> {
        // Do the math on the margin account.
        let span = error_span!("is_liquidatable");
        let mut col = I80F48::ZERO;

        // TODO: make as fn in margin_utils.
        for (i, &coll) in { margin.collateral }.iter().enumerate() {
            if coll == WrappedI80F48::zero() {
                continue;
            }

            let oracle =
                get_oracle(cache, &state.collaterals[i].oracle_symbol)?;
            let borrow_cache = cache.borrow_cache[i];
            let usdc_col = safe_mul_i80f48(coll.into(), oracle.price.into());

            let accrued = if coll > WrappedI80F48::zero() {
                safe_mul_i80f48(usdc_col, borrow_cache.supply_multiplier.into())
            } else {
                safe_mul_i80f48(usdc_col, borrow_cache.borrow_multiplier.into())
            };

            col = safe_add_i80f48(col, accrued);
        }

        let control = match table.get_control_from_margin(margin) {
            Some(pair) => pair.1,
            None => return Err(ErrorCode::InexistentControl),
        };

        // Have to rewrite this func to use current util instead of stored cache variables.
        // Also for multipliers.
        let cancel_result = check_fraction_requirement(
            FractionType::Cancel,
            col.to_num::<i64>(),
            table.state.total_markets as usize,
            table.state.total_collaterals as usize,
            &control.open_orders_agg,
            &table.state.perp_markets,
            &table.state.collaterals,
            &{ margin.collateral },
            &RefCell::new(table.cache).borrow(),
        );

        let result = check_fraction_requirement(
            FractionType::Maintenance,
            col.to_num::<i64>(),
            table.state.total_markets as usize,
            table.state.total_collaterals as usize,
            &control.open_orders_agg,
            &table.state.perp_markets,
            &table.state.collaterals,
            &{ margin.collateral },
            &RefCell::new(table.cache).borrow(),
        );

        let has_oo = has_open_orders(cache, control)?;
        match (cancel_result, result) {
            (Ok(is_not_cancel), Ok(is_not_liq)) => {
                Ok((!is_not_cancel, !is_not_liq && !has_oo))
            }
            (Ok(is_not_cancel), Err(e)) => {
                span.in_scope(|| {
                    error!("Error checking maintenance fraction: {:?}", e)
                });
                Ok((!is_not_cancel, false))
            }
            (Err(e), Ok(is_not_liq)) => {
                span.in_scope(|| {
                    error!("Error checking cancel fraction: {:?}", e)
                });
                Ok((false, !is_not_liq && !has_oo))
            }
            (Err(e1), Err(e2)) => {
                span.in_scope(|| {
                    error!("Error checking cancel fraction: {:?}", e1)
                });
                span.in_scope(|| {
                    error!("Error checking maintenance fraction: {:?}", e2)
                });
                Err(ErrorCode::LiquidationFailure)
            }
        }
    }

    pub fn get_clone(&self) -> Db {
        self.db.clone()
    }

    pub fn get(&self) -> &Db {
        &self.db
    }

    pub fn refresh_accounts(
        &self,
        opts: &Opts,
        payer_pubkey: &Pubkey,
    ) -> Result<(), ErrorCode> {
        let mut db = self.db.lock().unwrap();
        db.refresh_accounts(opts, payer_pubkey);
        Ok(())
    }
}
