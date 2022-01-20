use anchor_client::Program;

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
pub async fn liquidate_loop(st: &crate::AppState, database: DbWrapper) {
    info!("starting...");

    let mut last_refresh = std::time::Instant::now();
    let mut interval =
        tokio::time::interval(std::time::Duration::from_millis(250));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        let loop_start = std::time::Instant::now();
        match database
            .check_all_accounts(
                &st.client,
                &zo_abi::ID,
                &zo_abi::dex::ID,
                &zo_abi::serum::ID,
            )
            .await
        {
            Ok(n) => {
                debug!(
                    "Checked {} accounts in {} μs",
                    n,
                    loop_start.elapsed().as_micros()
                );
            }
            Err(e) => {
                error!("Had an oopsie-doopsie {:?}", e);
            }
        };

        if last_refresh.elapsed().as_secs() > 6000 {
            database.refresh_accounts(st).unwrap(); // TODO: Refactor this is bad.
            last_refresh = std::time::Instant::now();
            info!("Refreshed account table");
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
        true,
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

    // TODO: Priority queue for assets
    // [0, 1, 3, 2, 4, ...] loop through indixes and find first non-zero quote
    let quote_info: Option<(usize, &I80F48)> =
        match collateral_tuple.max_by_key(|a| a.1) {
            Some(x) => {
                if x.1.is_zero() {
                    Some((0, &I80F48::ZERO))
                } else {
                    Some(x)
                }
            }
            None => return Err(ErrorCode::NoCollateral),
        };

    // Sort the positions
    let positions: Vec<I80F48> = control
        .open_orders_agg
        .iter()
        .zip(cache.marks)
        .map(|(order, mark)| {
            safe_mul_i80f48(
                I80F48::from_num(order.pos_size.abs()),
                mark.price.into(),
            )
        })
        .collect();

    let positions = positions.iter().enumerate();

    let position: Option<(usize, &I80F48)> = match positions.max_by_key(|a| a.1)
    {
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

    let is_spot_bankrupt = colls.iter().all(|col| col < &DUST_THRESHOLD);

    if has_positions && (-min_col <= max_position_notional || is_spot_bankrupt)
    {
        info!(
            "Liquidating {}'s {} perp position",
            margin.authority, position_index
        );
        // Cancel a perp position
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

        liquidate_perp_position(
            program,
            payer_pubkey,
            payer_margin_key,
            payer_control_key,
            &payer_oo[position_index],
            margin,
            margin_key,
            &open_orders,
            cache_key,
            state_key,
            state_signer,
            dex_program,
            &market_info,
            &dex_market,
        )?;

        // rebalance on perp
        // call close perp position
        swap::close_position(
            program,
            state,
            state_key,
            state_signer,
            payer_margin,
            payer_margin_key,
            payer_control,
            &market_info,
            dex_program,
            position_index,
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
                serum_markets,
                serum_dex_program,
                serum_vault_signers,
            )?;
        };
    } else if *min_col < 0u64 {
        // Close a spot position
        let quote_idx = if let Some((q_idx, _q_coll)) = quote_info {
            q_idx
        } else {
            0
        };
        info!(
            "Liquidating {}'s {} spot position using {}",
            margin.authority, col_index, quote_idx
        );
        liquidate_spot_position(
            program,
            payer_pubkey,
            payer_margin_key,
            payer_control_key,
            margin_key,
            &margin.control,
            cache_key,
            state_key,
            &state.collaterals[col_index].mint,
            &state.collaterals[quote_idx].mint,
        )?;

        // rebalance on spot
        if let (Some(serum_market), Some(serum_vault_signer)) = (
            serum_markets.get(&quote_idx),
            serum_vault_signers.get(&quote_idx),
        ) {
            swap::swap_asset(
                program,
                payer_pubkey,
                state,
                state_key,
                state_signer,
                payer_margin_key,
                payer_control_key,
                serum_market,
                serum_dex_program,
                serum_vault_signer,
                quote_idx,
            )?;
        } else {
            warn!(
                "No serum market for {}. Not swapping for {}",
                quote_idx, margin.authority
            );
        }
        if let (Some(serum_market), Some(serum_vault_signer)) = (
            serum_markets.get(&col_index),
            serum_vault_signers.get(&col_index),
        ) {
            swap::swap_asset(
                program,
                payer_pubkey,
                state,
                state_key,
                state_signer,
                payer_margin_key,
                payer_control_key,
                serum_market,
                serum_dex_program,
                serum_vault_signer,
                col_index,
            )?;
        } else {
            warn!(
                "No serum market for {}. Not swapping for {}",
                col_index, margin.authority
            );
        }
    } else if let Some(_order_index) = largest_open_order(cache, control)? {
        // Must cancel perp open orders
        info!("Closing {}'s {} perp position", margin.authority, col_index);
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
    // TODO: Swap is a separate function called after liquidate.
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
                .args(instruction::ForceCancelAllPerpOrders { limit: 32 })
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
        Err(e) => {
            span.in_scope(|| error!("Failed to cancel perp position: {:?}", e));
            Err(ErrorCode::CancelFailure)
        }
    }
}

// Need the ix for liquidating a single account for a particular market.
fn liquidate_perp_position(
    program: &Program,
    payer_pubkey: &Pubkey,
    liqor_margin_key: &Pubkey,
    liqor_control_key: &Pubkey,
    liqor_oo_key: &Pubkey,
    liqee_margin: &Margin,
    liqee_margin_key: &Pubkey,
    liqee_open_orders: &Pubkey,
    cache_key: &Pubkey,
    state_key: &Pubkey,
    state_signer: &Pubkey,
    dex_program: &Pubkey,
    market_info: &MarketState,
    dex_market: &Pubkey,
) -> Result<(), ErrorCode> {
    let span = error_span!(
        "liquidate_perp_position",
        "{}",
        liqee_margin.authority.to_string()
    );
    // Can probably save some of these variables in the ds.
    // e.g. the state_signer and open_orders.

    let signature = retry_send(
        || {
            program
                .request()
                .accounts(ix_accounts::LiquidatePerpPosition {
                    state: *state_key,
                    cache: *cache_key,
                    state_signer: *state_signer,
                    liqor: *payer_pubkey,
                    liqor_margin: *liqor_margin_key,
                    liqor_control: *liqor_control_key,
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
                })
                .args(instruction::LiquidatePerpPosition {
                    asset_transfer_lots: u64::MAX / 1_000_000,
                })
                .options(CommitmentConfig::confirmed())
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
            Ok(())
        }
        Err(e) => {
            span.in_scope(|| {
                error!("Failed to liquidate perp position: {:?}", e)
            });
            Err(ErrorCode::LiquidationFailure)
        }
    }
}

fn liquidate_spot_position(
    program: &Program,
    payer_pubkey: &Pubkey,
    liqor_margin_key: &Pubkey,
    liqor_control_key: &Pubkey,
    liqee_margin_key: &Pubkey,
    liqee_control_key: &Pubkey,
    cache_key: &Pubkey,
    state_key: &Pubkey,
    asset_mint: &Pubkey,
    quote_mint: &Pubkey,
) -> Result<(), ErrorCode> {
    let span = error_span!("liquidate_spot_position");
    // Can probably save some of these variables in the ds.
    // e.g. the state_signer and open_orders.

    let signature = retry_send(
        || {
            program
                .request()
                .accounts(ix_accounts::LiquidateSpotPosition {
                    state: *state_key,
                    cache: *cache_key,
                    liqor: *payer_pubkey,
                    liqor_margin: *liqor_margin_key,
                    liqor_control: *liqor_control_key,
                    liqee_margin: *liqee_margin_key,
                    liqee_control: *liqee_control_key,
                    asset_mint: *asset_mint,
                    quote_mint: *quote_mint,
                })
                .args(instruction::LiquidateSpotPosition {
                    asset_transfer_amount: i64::MIN / 1_000_000,
                })
                .options(CommitmentConfig::confirmed())
        },
        5,
    );

    match signature {
        Ok(tx) => {
            span.in_scope(|| {
                info!("Liquidated {}'s spot. tx: {:?}", liqee_margin_key, tx)
            });
            Ok(())
        }
        Err(e) => {
            span.in_scope(|| {
                error!("Failed to liquidate spot position: {:?}", e)
            });
            Err(ErrorCode::LiquidationFailure)
        }
    }
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
    serum_markets: HashMap<usize, SerumMarketState>,
    serum_dex_program: &Pubkey,
    serum_vault_signers: HashMap<usize, Pubkey>,
) -> Result<(), ErrorCode> {
    let span = error_span!(
        "settle_bankruptcy",
        "{}",
        liqee_margin.authority.to_string()
    );
    let mut signature_results: Vec<Result<Signature, ErrorCode>> =
        Vec::with_capacity(MAX_COLLATERALS as usize);

    for (i, mint) in state.collaterals.iter().map(|c| &c.mint).enumerate() {
        if { liqee_margin.collateral[i] } >= WrappedI80F48::zero() {
            continue;
        }
        signature_results.push(retry_send(
            || {
                program
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
                    .options(CommitmentConfig::confirmed())
            },
            5,
        ));

        if let (Some(serum_market), Some(serum_vault_signer)) =
            (serum_markets.get(&i), serum_vault_signers.get(&i))
        {
            swap::swap_asset(
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
                i,
            )?;
        }
    }

    for (i, signature) in signature_results.iter().enumerate() {
        match signature {
            Ok(tx) => {
                span.in_scope(|| {
                    info!(
                        "Settled {}'s {} collateral. tx: {:?}",
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
