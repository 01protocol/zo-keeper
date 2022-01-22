/*
 * This file is responsible for handling swapping assets to USDC.
 * This is done after every liquidation to prevent risk exposure.
*/
use anchor_client::Program;

use anchor_lang::{
    prelude::ToAccountMetas, solana_program::instruction::Instruction,
    InstructionData,
};

use fixed::types::I80F48;

use serum_dex::{
    critbit::{NodeHandle, Slab, SlabView},
    state::MarketState as SerumMarketState,
};

use solana_sdk::{
    commitment_config::CommitmentConfig, pubkey::Pubkey,
    sysvar::rent::ID as RENT_ID,
};
use spl_token::ID as TOKEN_ID;

use std::cell::RefMut;

use tracing::{error, error_span, info, warn};

use zo_abi::{
    accounts, dex::ZoDexMarket as MarketState, instruction, Control, Margin,
    OrderType, State,
};

use crate::liquidator::{error::ErrorCode, math::SafeOp, utils::*};

pub fn swap_asset(
    program: &Program,
    payer: &Pubkey,
    state: &State,
    state_key: &Pubkey,
    state_signer: &Pubkey,
    payer_margin: &Pubkey,
    payer_control: &Pubkey,
    serum_market: &SerumMarketState,
    serum_dex_program: &Pubkey,
    serum_vault_signer: &Pubkey,
    asset_index: usize,
) -> Result<(), ErrorCode> {
    let span = error_span!("swap_asset", asset = asset_index);

    let quote_mint = state.collaterals[0].mint;
    let quote_vault = state.vaults[0];
    let asset_mint = state.collaterals[asset_index].mint;
    let asset_vault = state.vaults[asset_index];

    let client = program.rpc();

    let margin_account = client.get_account(payer_margin).unwrap();
    let col_index = 41 + asset_index * 16;
    let collateral: [u8; 16] = margin_account.data[col_index..col_index + 16]
        .to_vec()
        .try_into()
        .unwrap();
    let collateral_amount: I80F48 = I80F48::from_le_bytes(collateral);

    let buy = collateral_amount.is_negative();
    let swap_amount: u64 = if buy {
        let asks_key = array_to_pubkey(&{ serum_market.asks });
        let mut asks_account = client.get_account(&asks_key).unwrap();
        let asks_info = get_account_info(&asks_key, &mut asks_account);
        let asks: RefMut<Slab> = match serum_market.load_asks_mut(&asks_info) {
            Ok(asks) => asks,
            Err(e) => {
                span.in_scope(|| error!("Failed to fetch asks {}", e));
                return Err(ErrorCode::SwapError);
            }
        };

        let ask_handle: NodeHandle = match asks.find_min() {
            Some(min) => min,
            None => {
                span.in_scope(|| {
                    error!("No asks found for swapping {}", asset_index)
                });
                return Err(ErrorCode::NoAsks);
            }
        };

        let ask_price = match asks.get(ask_handle) {
            Some(ask) => ask.as_leaf().unwrap().price(),
            None => {
                span.in_scope(|| error!("Failed to fetch ask {}", asset_index));
                return Err(ErrorCode::SwapError);
            }
        };
        let factor: I80F48 = (I80F48::from(serum_market.pc_lot_size))
            .checked_div(I80F48::from(serum_market.coin_lot_size))
            .unwrap();
        let price: I80F48 = I80F48::from(u64::from(ask_price))
            .checked_mul(factor)
            .unwrap();

        collateral_amount
            .abs()
            .checked_mul(I80F48::from(
                (10u64)
                    .checked_pow(state.collaterals[asset_index].decimals as u32)
                    .unwrap(),
            ))
            .unwrap()
            .checked_mul(price)
            .unwrap()
            .to_num::<u64>()
    } else {
        999_999_999_999_999u64
    };

    if swap_amount <= 50 * 1000000 {
        // 50 USDC
        span.in_scope(|| warn!("No coins to swap for asset {}", asset_index));
        return Ok(());
    }

    let result = retry_send(
        || {
            program
                .request()
                .accounts(accounts::Swap {
                    authority: *payer,
                    state: *state_key,
                    state_signer: *state_signer,
                    cache: state.cache,
                    margin: *payer_margin,
                    control: *payer_control,
                    quote_mint,
                    quote_vault,
                    asset_mint,
                    asset_vault,
                    swap_fee_vault: state.swap_fee_vault,
                    serum_open_orders: state.collaterals[asset_index]
                        .serum_open_orders,
                    serum_market: array_to_pubkey(&{
                        serum_market.own_address
                    }),
                    serum_request_queue: array_to_pubkey(&{
                        serum_market.req_q
                    }),
                    serum_event_queue: array_to_pubkey(&{
                        serum_market.event_q
                    }),
                    serum_bids: array_to_pubkey(&{ serum_market.bids }),
                    serum_asks: array_to_pubkey(&{ serum_market.asks }),
                    serum_coin_vault: array_to_pubkey(&{
                        serum_market.coin_vault
                    }),
                    serum_pc_vault: array_to_pubkey(&{ serum_market.pc_vault }),
                    serum_vault_signer: *serum_vault_signer,
                    srm_spot_program: *serum_dex_program,
                    token_program: TOKEN_ID,
                    rent: RENT_ID,
                })
                .args(instruction::Swap {
                    buy,
                    allow_borrow: false,
                    amount: swap_amount,
                    min_rate: 1u64, // WARNING: this can have a lot of slippage
                })
                .options(CommitmentConfig::confirmed())
        },
        5,
    );

    match result {
        Ok(_tx) => Ok(()),
        Err(e) => {
            span.in_scope(|| error!("Failed to swap asset {:?}", e));
            Err(ErrorCode::SwapError)
        }
    }
}

pub fn close_position(
    program: &Program,
    state: &State,
    state_key: &Pubkey,
    state_signer: &Pubkey,
    margin: &Margin,
    margin_key: &Pubkey,
    control: &Control,
    dex_market: &MarketState,
    dex_program: &Pubkey,
    index: usize,
) -> Result<(), ErrorCode> {
    // Pick the right market and place a market order to close the position you received from liquidating someone.
    // Need to know the amount to close
    let client = program.rpc();

    let oo_account = client
        .get_account(&control.open_orders_agg[index].key)
        .unwrap();

    let native_coin_total_bytes: [u8; 8] =
        oo_account.data[85..93].to_vec().try_into().unwrap();

    let native_coin_total = i64::from_le_bytes(native_coin_total_bytes);
    let span = error_span!("close_position", index = index);

    if native_coin_total == 0 {
        span.in_scope(|| warn!("No coins to close for asset {}", index));
        return Ok(());
    }

    let result = if native_coin_total < 0 {
        // Short order
        retry_send(
            || {
                program
                    .request()
                    .accounts(accounts::PlacePerpOrder {
                        state: *state_key,
                        state_signer: *state_signer,
                        cache: state.cache,
                        authority: margin.authority,
                        margin: *margin_key,
                        control: margin.control,
                        open_orders: control.open_orders_agg[index].key,
                        dex_market: dex_market.own_address,
                        req_q: dex_market.req_q,
                        event_q: dex_market.event_q,
                        market_bids: dex_market.bids,
                        market_asks: dex_market.asks,
                        dex_program: *dex_program,
                        rent: RENT_ID,
                    })
                    .args(instruction::PlacePerpOrder {
                        is_long: true,                       // Long to cancel it out
                        limit_price: 999_999_999_999_999u64, // TODO: make this more principled
                        max_base_quantity: (native_coin_total.abs() as u64)
                            .safe_div(dex_market.coin_lot_size)
                            .unwrap(),
                        max_quote_quantity: 999_999_999_999_999u64,
                        order_type: OrderType::ReduceOnlyIoc,
                        limit: 10,
                        client_id: 0u64,
                    })
                    .options(CommitmentConfig::confirmed())
            },
            5,
        )
    } else {
        // Long order
        retry_send(
            || {
                program
                    .request()
                    .accounts(accounts::PlacePerpOrder {
                        state: *state_key,
                        state_signer: *state_signer,
                        cache: state.cache,
                        authority: margin.authority,
                        margin: *margin_key,
                        control: margin.control,
                        open_orders: control.open_orders_agg[index].key,
                        dex_market: dex_market.own_address,
                        req_q: dex_market.req_q,
                        event_q: dex_market.event_q,
                        market_bids: dex_market.bids,
                        market_asks: dex_market.asks,
                        dex_program: *dex_program,
                        rent: RENT_ID,
                    })
                    .args(instruction::PlacePerpOrder {
                        is_long: false,    // Short to cancel it out
                        limit_price: 1u64, // TODO: make this more principled
                        max_base_quantity: (native_coin_total as u64)
                            .safe_div(dex_market.coin_lot_size)
                            .unwrap(),
                        max_quote_quantity: 1u64,
                        order_type: OrderType::Limit,
                        limit: 10,
                        client_id: 0u64,
                    })
                    .options(CommitmentConfig::confirmed())
            },
            5,
        )
    };

    match result {
        Ok(tx) => {
            span.in_scope(|| {
                info!("Successfully placed order to close position {:?}", tx)
            });
            Ok(())
        }
        Err(e) => {
            span.in_scope(|| error!("Failed to close position: {:?}", e));
            Err(ErrorCode::SwapError)
        }
    }
}

pub fn close_position_ix(
    program: &Program,
    state: &State,
    state_key: &Pubkey,
    state_signer: &Pubkey,
    margin: &Margin,
    margin_key: &Pubkey,
    control: &Control,
    dex_market: &MarketState,
    dex_program: &Pubkey,
    index: usize,
    liqee_was_long: bool,
) -> Result<Instruction, ErrorCode> {

    // Close all perp positions
    let limit: u64 = if !liqee_was_long {
        999_999_999_999_999
    } else {
        1
    };

    let close_ix = Instruction {
        accounts: accounts::PlacePerpOrder {
            state: *state_key,
            state_signer: *state_signer,
            cache: state.cache,
            authority: margin.authority,
            margin: *margin_key,
            control: margin.control,
            open_orders: control.open_orders_agg[index].key,
            dex_market: dex_market.own_address,
            req_q: dex_market.req_q,
            event_q: dex_market.event_q,
            market_bids: dex_market.bids,
            market_asks: dex_market.asks,
            dex_program: *dex_program,
            rent: RENT_ID,
        }
        .to_account_metas(None),
        data: instruction::PlacePerpOrder {
            is_long: !liqee_was_long,   // Place opposite order to close
            limit_price: limit, // TODO: make this more principled
            max_base_quantity: 999_999_999_999_999u64,
            max_quote_quantity: 999_999_999_999_999u64,
            order_type: OrderType::ReduceOnlyIoc,
            limit: 10,
            client_id: 0u64,
        }
        .data(),
        program_id: program.id(),
    };

    Ok(close_ix)
}
