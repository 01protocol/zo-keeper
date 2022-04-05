use anchor_client::Program;

use anchor_lang::{
    prelude::ToAccountMetas, solana_program::instruction::Instruction,
    InstructionData,
};

use fixed::types::I80F48;

use serum_dex::state::MarketState as SerumMarketState;

use solana_sdk::{
    commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Signature,
};

use std::collections::HashMap;

use zo_abi::{
    accounts as ix_accounts, dex::ZoDexMarket as MarketState, instruction,
    Cache, Control, Margin, State, WrappedI80F48, DUST_THRESHOLD,
    MAX_COLLATERALS, MAX_MARKETS,
};

use std::cell::RefCell;

use tracing::{debug, error, error_span, info, warn};

use crate::liquidator::{
    accounts::*, error::ErrorCode, margin_utils::*, math::*, swap, utils::*,
};

#[tracing::instrument(skip_all, level = "error")]
pub async fn liquidate_loop(st: &'static crate::AppState, database: DbWrapper) {
    info!("starting liquidator v0.1.0...");

    let mut last_refresh = std::time::Instant::now();
    let mut interval =
        tokio::time::interval(std::time::Duration::from_millis(250));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        let loop_start = std::time::Instant::now();
        match database
            .check_all_accounts(
                &st,
                &zo_abi::ZO_DEX_PID,
                &zo_abi::SERUM_DEX_PID,
            )
            .await
        {
            Ok(n) => {
                debug!(
                    "Checked {} accounts in {} ms",
                    n,
                    loop_start.elapsed().as_millis()
                );
            }
            Err(e) => {
                error!("Had an oopsie-doopsie {:?}", e);
            }
        };

        if last_refresh.elapsed().as_secs() > 300 {
            match database.refresh_accounts(st) {
                Ok(_) => info!("Refreshed account table"),
                Err(e) => warn!("Failed to refresh: {}", e),
            }
            last_refresh = std::time::Instant::now();
        }
    }
}

#[tracing::instrument(
    skip_all,
    level = "error",
    fields(authority = %margin.authority),
)]
pub fn liquidate(
    program: &Program,
    dex_program: &Pubkey,
    payer_pubkey: &Pubkey,
    payer_margin: &Margin,
    payer_margin_key: &Pubkey,
    payer_control: &Control,
    payer_control_key: &Pubkey,
    payer_oo: &[Pubkey; MAX_MARKETS as usize],
    margin_key: &Pubkey,
    margin: &Margin,
    control: &Control,
    cache: &Cache,
    cache_key: &Pubkey,
    state: &State,
    state_key: &Pubkey,
    state_signer: &Pubkey,
    market_infos: Vec<MarketState>,
    serum_markets: HashMap<usize, SerumMarketState>,
    serum_dex_program: &Pubkey,
    serum_vault_signers: HashMap<usize, Pubkey>,
) -> Result<(), ErrorCode> {
    // Given an account to liquidate
    // Go through its positions and pick the largest one.
    // Liquidate that position.

    // Start by sorting the collateral
    let colls = get_actual_collateral_vec(
        margin,
        &RefCell::new(*state).borrow(),
        &RefCell::new(*cache).borrow(),
        false,
    );

    let colls = match colls {
        Ok(colls) => colls,
        Err(e) => {
            error!(
                "Failed to calculate collateral for {}: {:?}",
                margin.authority, e
            );
            return Err(ErrorCode::CollateralFailure);
        }
    };
    let collateral_tuple = colls.iter().enumerate();
    let (col_index, min_col) =
        match collateral_tuple.clone().min_by_key(|a| a.1) {
            Some(x) => x,
            None => return Err(ErrorCode::NoCollateral),
        };

    // Find the highest weighted asset that is positive.
    let mut quote_info: Option<(usize, &I80F48)> = None;
    let mut current_weight = 1000;
    for (i, coll) in collateral_tuple {
        if coll > &I80F48::from_num(DUST_THRESHOLD)
            && state.collaterals[i].weight <= current_weight
        {
            current_weight = state.collaterals[i].weight;
            quote_info = Some((i, &coll));
        }
    }

    // Sort the positions
    let positions: Vec<I80F48> = control
        .open_orders_agg
        .iter()
        .zip(cache.marks)
        .map(|(order, mark)| {
            safe_mul_i80f48(I80F48::from_num(order.pos_size), mark.price.into())
        })
        .collect();

    let positions = positions.iter().enumerate();

    let position: Option<(usize, &I80F48)> =
        match positions.max_by_key(|a| a.1.abs()) {
            Some(x) => {
                if x.1.is_zero() {
                    None
                } else {
                    Some(x)
                }
            }
            None => return Err(ErrorCode::NoPositions),
        };

    // Pick the larger one, liquidate
    let has_positions: bool;
    let position_index: usize;
    let max_position_notional: I80F48;
    if let Some((pos_index, &max_pos_notional)) = position {
        has_positions = true;
        position_index = pos_index;
        max_position_notional = max_pos_notional;
    } else {
        has_positions = false;
        position_index = 0;
        max_position_notional = I80F48::ZERO;
    }
    let dex_market = state.perp_markets[position_index].dex_market;

    let (open_orders, _nonce) = Pubkey::find_program_address(
        &[&margin.control.to_bytes()[..], &dex_market.to_bytes()[..]],
        dex_program,
    );
    let market_info = market_infos[position_index];

    let is_spot_bankrupt = colls.iter().all(|col| col < &DUST_THRESHOLD)
        && colls.iter().sum::<I80F48>().is_negative();

    if has_positions
        && (min_col.abs() <= max_position_notional.abs() || is_spot_bankrupt)
    {
        liquidate_perp_position(
            program,
            payer_pubkey,
            payer_margin,
            payer_margin_key,
            payer_control,
            &payer_oo[position_index],
            margin,
            margin_key,
            &open_orders,
            cache,
            cache_key,
            state,
            state_key,
            state_signer,
            dex_program,
            &market_info,
            &dex_market,
            position_index,
            max_position_notional.is_positive(),
        )?;
    } else if is_spot_bankrupt && !has_positions {
        let oo_index_result = largest_open_order(cache, control)?;

        if let Some(_order_index) = oo_index_result {
            cancel(
                program,
                dex_program,
                payer_pubkey,
                margin_key,
                margin,
                control,
                cache,
                cache_key,
                state,
                state_key,
                state_signer,
                market_infos,
            )?;
        } else {
            settle_bankruptcy(
                program,
                state,
                state_key,
                state_signer,
                cache_key,
                payer_pubkey,
                payer_margin_key,
                payer_control_key,
                margin,
                margin_key,
                colls,
                serum_markets,
                serum_dex_program,
                serum_vault_signers,
            )?;
        };
    } else if *min_col < 0u64 && quote_info.is_some() {
        // Close a spot position
        let quote_idx = if let Some((q_idx, _q_coll)) = quote_info {
            q_idx
        } else {
            0
        };

        liquidate_spot_position(
            program,
            payer_pubkey,
            payer_margin,
            payer_margin_key,
            payer_control,
            margin,
            margin_key,
            control,
            cache,
            cache_key,
            state,
            state_key,
            state_signer,
            col_index,
            quote_idx,
            serum_markets,
            serum_dex_program,
            serum_vault_signers,
        )?;
    } else if let Some(_order_index) = largest_open_order(cache, control)? {
        // Must cancel perp open orders
        info!("Closing {}'s {} perp order", margin.authority, col_index);
        cancel(
            program,
            dex_program,
            payer_pubkey,
            margin_key,
            margin,
            control,
            cache,
            cache_key,
            state,
            state_key,
            state_signer,
            market_infos,
        )?;
    }

    // TODO: Refactor so that you return an enum
    // TODO: enum specifies swap type and relevant params.
    Ok(())
}

pub fn cancel(
    program: &Program,
    dex_program: &Pubkey,
    payer_pubkey: &Pubkey,
    margin_key: &Pubkey,
    margin: &Margin,
    control: &Control,
    cache: &Cache,
    cache_key: &Pubkey,
    state: &State,
    state_key: &Pubkey,
    state_signer: &Pubkey,
    market_info: Vec<MarketState>,
) -> Result<(), ErrorCode> {
    let span = error_span!("cancel");

    let oo_index_result = largest_open_order(cache, control)?;

    let oo_index: usize = if let Some(order_index) = oo_index_result {
        order_index
    } else {
        span.in_scope(|| {
            debug!("No open orders to cancel for {}", margin.authority)
        });
        return Ok(());
    };

    let dex_market = state.perp_markets[oo_index].dex_market;
    let (open_orders, _nonce) = Pubkey::find_program_address(
        &[&margin.control.to_bytes()[..], &dex_market.to_bytes()[..]],
        dex_program,
    );
    let market_info = market_info[oo_index];

    cancel_orders(
        program,
        payer_pubkey,
        margin_key,
        &margin.control,
        cache_key,
        state_key,
        state_signer,
        &open_orders,
        &market_info.own_address,
        &market_info.req_q,
        &market_info.event_q,
        &market_info.bids,
        &market_info.asks,
        dex_program,
    )?;

    Ok(())
}

fn cancel_orders(
    program: &Program,
    payer_pubkey: &Pubkey,
    margin_key: &Pubkey,
    control_key: &Pubkey,
    cache_key: &Pubkey,
    state_key: &Pubkey,
    state_signer: &Pubkey,
    open_orders: &Pubkey,
    dex_market: &Pubkey,
    req_q: &Pubkey,
    event_q: &Pubkey,
    market_bids: &Pubkey,
    market_asks: &Pubkey,
    dex_program: &Pubkey,
) -> Result<(), ErrorCode> {
    // Can probably save some of these variables in the ds.
    // e.g. the state_signer and open_orders.

    let span = error_span!("cancel_orders");
    let signature = retry_send(
        || {
            program
                .request()
                .accounts(ix_accounts::ForceCancelAllPerpOrders {
                    pruner: *payer_pubkey,
                    state: *state_key,
                    cache: *cache_key,
                    state_signer: *state_signer,
                    liqee_margin: *margin_key,
                    liqee_control: *control_key,
                    liqee_oo: *open_orders,
                    dex_market: *dex_market,
                    req_q: *req_q,
                    event_q: *event_q,
                    market_bids: *market_bids,
                    market_asks: *market_asks,
                    dex_program: *dex_program,
                })
                .args(instruction::ForceCancelAllPerpOrders { limit: 300 })
                .options(CommitmentConfig::confirmed())
        },
        5,
    );

    match signature {
        Ok(tx) => {
            span.in_scope(|| {
                info!("Cancelled {}'s open orders. tx: {:?}", margin_key, tx)
            });
            Ok(())
        }
        Err(_e) => Err(ErrorCode::CancelFailure),
    }
}

// Need the ix for liquidating a single account for a particular market.
fn liquidate_perp_position(
    program: &Program,
    payer_pubkey: &Pubkey,
    liqor_margin: &Margin,
    liqor_margin_key: &Pubkey,
    liqor_control: &Control,
    liqor_oo_key: &Pubkey,
    liqee_margin: &Margin,
    liqee_margin_key: &Pubkey,
    liqee_open_orders: &Pubkey,
    cache: &Cache,
    cache_key: &Pubkey,
    state: &State,
    state_key: &Pubkey,
    state_signer: &Pubkey,
    dex_program: &Pubkey,
    market_info: &MarketState,
    dex_market: &Pubkey,
    index: usize,
    liqee_was_long: bool,
) -> Result<(), ErrorCode> {
    let span = error_span!(
        "liquidate_perp_position",
        "{}",
        liqee_margin.authority.to_string()
    );
    // Can probably save some of these variables in the ds.
    // e.g. the state_signer and open_orders.

    let cancel_ix = Instruction {
        accounts: ix_accounts::ForceCancelAllPerpOrders {
            pruner: *payer_pubkey,
            state: *state_key,
            cache: *cache_key,
            state_signer: *state_signer,
            liqee_margin: *liqee_margin_key,
            liqee_control: liqee_margin.control,
            liqee_oo: *liqee_open_orders,
            dex_market: *dex_market,
            req_q: market_info.req_q,
            event_q: market_info.event_q,
            market_bids: market_info.bids,
            market_asks: market_info.asks,
            dex_program: *dex_program,
        }
        .to_account_metas(None),
        data: instruction::ForceCancelAllPerpOrders { limit: 300 }.data(),
        program_id: program.id(),
    };

    let mut asset_transfer_lots =
        get_total_account_value(liqor_margin, liqor_control, state, cache)
            .checked_div(cache.marks[index].price.into())
            .unwrap()
            .to_num::<i64>()
            .safe_div(market_info.coin_lot_size)
            .unwrap()
            .safe_mul(5i64) // 5x leverage
            .unwrap();

    debug!(
        "{} | {} {}",
        liqee_margin.authority,
        asset_transfer_lots,
        String::from(state.perp_markets[index].symbol)
    );

    let mut liq_ix = Instruction {
        accounts: ix_accounts::LiquidatePerpPosition {
            state: *state_key,
            cache: *cache_key,
            state_signer: *state_signer,
            liqor: *payer_pubkey,
            liqor_margin: *liqor_margin_key,
            liqor_control: liqor_margin.control,
            liqor_oo: *liqor_oo_key,
            liqee: liqee_margin.authority,
            liqee_margin: *liqee_margin_key,
            liqee_control: liqee_margin.control,
            liqee_oo: *liqee_open_orders,
            dex_market: *dex_market,
            req_q: market_info.req_q,
            event_q: market_info.event_q,
            market_bids: market_info.bids,
            market_asks: market_info.asks,
            dex_program: *dex_program,
        }
        .to_account_metas(None),
        data: instruction::LiquidatePerpPosition {
            asset_transfer_lots: asset_transfer_lots as u64,
        }
        .data(),
        program_id: program.id(),
    };

    let rebalance_ix: Option<Instruction> = match swap::close_position_ix(
        program,
        state,
        state_key,
        state_signer,
        liqor_margin,
        liqor_margin_key,
        liqor_control,
        market_info,
        dex_program,
        index,
        liqee_was_long,
    ) {
        Ok(ix) => Some(ix),
        Err(_e) => {
            span.in_scope(|| warn!("Unable to create rebalance instruction"));
            None
        }
    };

    let reduction_max = 5;

    let mut signature;
    for _reduction in 0..reduction_max {
        signature = retry_send(
            || {
                let request = program
                    .request()
                    .instruction(cancel_ix.clone())
                    .instruction(liq_ix.clone())
                    .options(CommitmentConfig::confirmed());
                if let Some(ix) = rebalance_ix.clone() {
                    request.instruction(ix)
                } else {
                    request
                }
            },
            5,
        );

        match signature {
            Ok(tx) => {
                span.in_scope(|| {
                    info!(
                        "Liquidated {}'s perp. tx: {:?}",
                        liqee_margin.authority, tx
                    )
                });
                return Ok(());
            }
            Err(e) => match e {
                ErrorCode::LiquidationOverExposure => {
                    asset_transfer_lots /= 2;
                    liq_ix.data = instruction::LiquidatePerpPosition {
                        asset_transfer_lots: asset_transfer_lots as u64,
                    }
                    .data();
                }
                _ => {
                    return Err(ErrorCode::LiquidationFailure);
                }
            },
        }
    }

    Err(ErrorCode::LiquidationFailure)
}

fn liquidate_spot_position(
    program: &Program,
    payer_pubkey: &Pubkey,
    liqor_margin: &Margin,
    liqor_margin_key: &Pubkey,
    liqor_control: &Control,
    liqee_margin: &Margin,
    liqee_margin_key: &Pubkey,
    liqee_control: &Control,
    cache: &Cache,
    cache_key: &Pubkey,
    state: &State,
    state_key: &Pubkey,
    state_signer: &Pubkey,
    asset_index: usize,
    quote_index: usize,
    serum_markets: HashMap<usize, SerumMarketState>,
    serum_dex_program: &Pubkey,
    serum_vault_signers: HashMap<usize, Pubkey>,
) -> Result<(), ErrorCode> {
    let span = error_span!("liquidate_spot_position");

    let asset_collateral_info = state.collaterals[asset_index];
    let quote_collateral_info = state.collaterals[quote_index];

    let quote_price: I80F48 =
        get_oracle(cache, &quote_collateral_info.oracle_symbol)
            .unwrap()
            .price
            .into();

    let asset_price: I80F48 =
        get_oracle(cache, &asset_collateral_info.oracle_symbol)
            .unwrap()
            .price
            .into();

    let asset_transfer_lots =
        get_total_account_value(liqor_margin, liqor_control, state, cache)
            * I80F48::from_num(5u8);

    let size_estimate = estimate_spot_liquidation_size(
        liqee_margin,
        liqee_control,
        state,
        cache,
        asset_index,
        quote_index,
    );

    let fudge = I80F48::from_str_binary("1.1").unwrap();
    let mut usdc_amount = match size_estimate {
        Some(size_estimate) => {
            let amount = size_estimate * fudge;
            amount.min(asset_transfer_lots)
        }
        None => I80F48::ZERO,
    };

    debug!(
        "{}: {}sUSD s{} -> s{}",
        liqee_margin.authority,
        usdc_amount / quote_price,
        String::from(quote_collateral_info.oracle_symbol),
        String::from(asset_collateral_info.oracle_symbol),
    );

    let mut liq_ix = Instruction {
        accounts: ix_accounts::LiquidateSpotPosition {
            state: *state_key,
            cache: *cache_key,
            liqor: *payer_pubkey,
            liqor_margin: *liqor_margin_key,
            liqor_control: liqor_margin.control,
            liqee_margin: *liqee_margin_key,
            liqee_control: liqee_margin.control,
            asset_mint: asset_collateral_info.mint,
            quote_mint: quote_collateral_info.mint,
        }
        .to_account_metas(None),
        data: instruction::LiquidateSpotPosition {
            asset_transfer_amount: -usdc_amount
                .unwrapped_div(asset_price)
                .to_num::<i64>(),
        }
        .data(),
        program_id: program.id(),
    };

    let mut swap_ixs: Vec<Instruction> = Vec::new();

    if let (Some(serum_market), Some(serum_vault_signer)) = (
        serum_markets.get(&quote_index),
        serum_vault_signers.get(&quote_index),
    ) {
        // Rebalance the quote (which is what was received)
        // Make sure that it's not a zero-transfer
        if usdc_amount.abs() / quote_price
            > I80F48::from_num(2 * serum_market.coin_lot_size)
        {
            debug!(
                "Rebalancing {} s{}",
                usdc_amount,
                String::from(asset_collateral_info.oracle_symbol)
            );
            let remove_quote = swap::make_swap_ix(
                program,
                payer_pubkey,
                state,
                state_key,
                state_signer,
                liqor_margin_key,
                &liqor_margin.control,
                serum_market,
                serum_dex_program,
                serum_vault_signer,
                999_999_999_999_999u64,
                false,
                quote_index,
            )?;

            swap_ixs.push(remove_quote);
        }
    }

    if let (Some(serum_market), Some(serum_vault_signer)) = (
        serum_markets.get(&asset_index),
        serum_vault_signers.get(&asset_index),
    ) {
        // Rebalance the asset (which is what was given)
        if usdc_amount.abs() / asset_price
            >= I80F48::from_num(2 * serum_market.coin_lot_size)
                * (I80F48::ONE / (fudge - I80F48::ONE) + I80F48::ONE)
        {
            debug!(
                "Rebalancing {} s{}",
                usdc_amount / asset_price,
                String::from(asset_collateral_info.oracle_symbol)
            );
            let remove_debt = swap::make_swap_ix(
                // amount is what is what is being sold always usdc here
                program,
                payer_pubkey,
                state,
                state_key,
                state_signer,
                liqor_margin_key,
                &liqor_margin.control,
                serum_market,
                serum_dex_program,
                serum_vault_signer,
                usdc_amount.ceil().to_num(),
                true,
                asset_index,
            )?;

            let remove_excess = swap::make_swap_ix(
                program,
                payer_pubkey,
                state,
                state_key,
                state_signer,
                liqor_margin_key,
                &liqor_margin.control,
                serum_market,
                serum_dex_program,
                serum_vault_signer,
                999_999_999_999_999u64,
                false,
                asset_index,
            )?;

            swap_ixs.push(remove_debt);
            swap_ixs.push(remove_excess);
        }
    }

    let reduction_max = 5;
    for _reduction in 0..reduction_max {
        let signature = retry_send(
            || {
                let mut request_builder = program
                    .request()
                    .instruction(liq_ix.clone())
                    .options(CommitmentConfig::confirmed());

                for ix in swap_ixs.clone() {
                    request_builder = request_builder.instruction(ix);
                }
                request_builder
            },
            5,
        );

        match signature {
            Ok(tx) => {
                span.in_scope(|| {
                    info!(
                        "Liquidated {}'s spot. tx: {:?}",
                        liqee_margin.authority, tx
                    )
                });
                return Ok(());
            }
            Err(e) => match e {
                ErrorCode::LiquidationOverExposure => {
                    usdc_amount /= 2;
                    liq_ix.data = instruction::LiquidateSpotPosition {
                        asset_transfer_amount: -(usdc_amount / asset_price)
                            .to_num::<i64>(),
                    }
                    .data();
                }
                _ => {
                    return Err(ErrorCode::LiquidationFailure);
                }
            },
        }
    }
    return Err(ErrorCode::LiquidationFailure);
}

fn settle_bankruptcy(
    program: &Program,
    state: &State,
    state_key: &Pubkey,
    state_signer: &Pubkey,
    cache_key: &Pubkey,
    liqor_key: &Pubkey,
    liqor_margin_key: &Pubkey,
    liqor_control_key: &Pubkey,
    liqee_margin: &Margin,
    liqee_margin_key: &Pubkey,
    liqee_colls: Vec<I80F48>,
    serum_markets: HashMap<usize, SerumMarketState>,
    serum_dex_program: &Pubkey,
    serum_vault_signers: HashMap<usize, Pubkey>,
) -> Result<(), ErrorCode> {
    let span = error_span!(
        "settle_bankruptcy",
        "{}",
        liqee_margin.authority.to_string()
    );
    let mut signature_results: Vec<(usize, Result<Signature, ErrorCode>)> =
        Vec::with_capacity(MAX_COLLATERALS as usize);

    for (i, mint) in state.collaterals.iter().map(|c| &c.mint).enumerate() {
        if { liqee_margin.collateral[i] } >= WrappedI80F48::zero()
            || mint.eq(&Pubkey::default())
        {
            continue;
        }

        let swap: Option<Instruction> =
            if let (Some(serum_market), Some(serum_vault_signer)) =
                (serum_markets.get(&i), serum_vault_signers.get(&i))
            {
                let amount: u64 = liqee_colls[i].abs().to_num();
                if amount == 0 || amount <= 2 * serum_market.coin_lot_size {
                    None
                } else {
                    Some(swap::make_swap_ix(
                        program,
                        liqor_key,
                        state,
                        state_key,
                        state_signer,
                        liqor_margin_key,
                        liqor_control_key,
                        serum_market,
                        serum_dex_program,
                        serum_vault_signer,
                        amount,
                        true,
                        i,
                    )?)
                }
            } else {
                None
            };

        signature_results.push((
            i,
            retry_send(
                || {
                    let request_builder = program
                        .request()
                        .accounts(ix_accounts::SettleBankruptcy {
                            state: *state_key,
                            state_signer: *state_signer,
                            cache: *cache_key,
                            liqor: *liqor_key,
                            liqor_margin: *liqor_margin_key,
                            liqor_control: *liqor_control_key,
                            liqee_margin: *liqee_margin_key,
                            liqee_control: liqee_margin.control,
                            asset_mint: *mint,
                        })
                        .args(instruction::SettleBankruptcy {})
                        .options(CommitmentConfig::confirmed());

                    match swap.clone() {
                        Some(ix) => request_builder.instruction(ix),
                        None => request_builder,
                    }
                },
                5,
            ),
        ));
    }

    for (i, signature) in signature_results.iter() {
        match signature {
            Ok(tx) => {
                span.in_scope(|| {
                    info!(
                        "Settled margin {}'s {} collateral. tx: {:?}",
                        liqee_margin_key, i, tx
                    )
                });
            }
            Err(e) => {
                span.in_scope(|| {
                    error!(
                        "Failed to settle bankruptcy for asset {}: {:?}",
                        i, e
                    )
                });
                return Err(ErrorCode::SettlementFailure);
            }
        }
    }

    Ok(())
}
