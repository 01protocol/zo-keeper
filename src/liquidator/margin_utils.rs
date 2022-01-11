use fixed::types::I80F48;

use solana_sdk::pubkey::Pubkey;

use std::{cell::Ref, cmp};

use zo_abi::{
    Cache, CollateralInfo, Control, FractionType, Margin, MarkCache,
    OpenOrdersInfo, PerpMarketInfo, State, WrappedI80F48, MAX_COLLATERALS,
    MAX_MARKETS, SPOT_INITIAL_MARGIN_REQ, SPOT_MAINT_MARGIN_REQ,
};

use crate::liquidator::{error::ErrorCode, math::*, utils::*};

pub fn check_fraction_requirement(
    fraction_type: FractionType,
    col: i64,
    max_markets: usize,
    max_cols: usize,
    oo_agg: &[OpenOrdersInfo; MAX_MARKETS as usize],
    pm: &[PerpMarketInfo; MAX_MARKETS as usize],
    col_info_arr: &[CollateralInfo; MAX_COLLATERALS as usize],
    margin_col: &[WrappedI80F48; MAX_COLLATERALS as usize],
    cache: &Ref<Cache>,
) -> Result<bool, ErrorCode> {
    let (
        total_acc_value,
        mut has_open_pos_notional,
        total_realized_pnl,
        mut mf_vec,
        mut pos_open_notional_vec,
    ) = get_perp_acc_params(
        col,
        fraction_type,
        max_markets,
        oo_agg,
        &cache.marks,
        pm,
        &{ cache.funding_cache },
    )?;

    let (has_spot_pos_notional, mut spot_mf_vec, mut spot_pos_notional_vec) =
        get_spot_borrows(
            fraction_type == FractionType::Initial,
            max_cols,
            margin_col,
            col_info_arr,
            cache,
            total_realized_pnl,
        )?;

    if has_spot_pos_notional {
        has_open_pos_notional = true;
    }
    // has_open_pos_notional = has_open_pos_notional.safe_add(spot_pos_notional)?;

    mf_vec.append(&mut spot_mf_vec);
    pos_open_notional_vec.append(&mut spot_pos_notional_vec);
    //println!("mf : {:?}. mmf : {:?}", mf_vec, pos_open_notional_vec);
    match fraction_type {
        FractionType::Initial => {
            if has_open_pos_notional {
                // OMF = min(total account value, collateral + realized pnl) / total open position notional
                // ignoring the division by total_open_pos_notional because imf is also divided, so redundant division
                let omf = total_acc_value
                    .min(col + total_realized_pnl)
                    .safe_mul(1000i64)
                    .unwrap();

                let imf =
                    calc_weighted_sum(mf_vec, pos_open_notional_vec).unwrap();

                Ok(omf > imf)
            } else {
                Ok(true)
            }
        }
        FractionType::Maintenance => {
            if has_open_pos_notional {
                // MF = total_account_value / total_pos_open_notional
                // ignoring the division by total_open_pos_notional because imf is also divided, so redundant division
                let mf = total_acc_value.safe_mul(1000i64).unwrap();
                // msg!("total_acc_value {}", total_acc_value);
                // msg!("total_open_pos_notional {}", has_open_pos_notional);

                let mmf =
                    calc_weighted_sum(mf_vec, pos_open_notional_vec).unwrap();
                //println!("account {}. mf {}. mmf {}", col, mf, mmf);
                Ok(mf > mmf)
            } else {
                //println!("account {}. acc_val {}", col, total_acc_value);
                Ok(true)
            }
        }
        FractionType::Cancel => {
            if has_open_pos_notional {
                // OMF = min(total account value, collateral + realized pnl) / total open position notional
                // ignoring the division by total_open_pos_notional because imf is also divided, so redundant division
                let omf = total_acc_value
                    .min(col + total_realized_pnl)
                    .safe_mul(1000i64)
                    .unwrap();

                let cmf =
                    calc_weighted_sum(mf_vec, pos_open_notional_vec).unwrap();
                //println!("account {}. omf {}. cmf {}", col, omf, cmf);
                Ok(omf > cmf)
            } else {
                //println!("account {}. acc_val {}", col, total_acc_value);
                Ok(true)
            }
        }
    }
}

fn get_perp_acc_params(
    col: i64,
    fraction_type: FractionType,
    max_markets: usize,
    open_orders_agg: &[OpenOrdersInfo; 50],
    marks: &[MarkCache; 50],
    perp_markets: &[PerpMarketInfo; 50],
    funding_cache: &[i128; 50],
) -> Result<(i64, bool, i64, Vec<u16>, Vec<i64>), ErrorCode> {
    // for omf
    let mut total_acc_value = col;
    let mut has_open_pos_notional = false;
    let mut total_realized_pnl = 0i64;

    // for imf or mmf
    let mut mf_vec = Vec::new();
    let mut pos_open_notional_vec = Vec::new();

    for (index, oo_info) in open_orders_agg.iter().enumerate() {
        if index >= max_markets {
            break;
        }
        if oo_info.key == Pubkey::default() {
            continue;
        }

        let mark = marks[index].price.into();

        let new_acc_val = calc_acc_val(
            total_acc_value,
            mark,
            oo_info.pos_size,
            oo_info.native_pc_total,
            oo_info.realized_pnl,
            oo_info.funding_index,
            funding_cache[index],
            perp_markets[index].asset_decimals as u32,
        )?;
        total_acc_value = new_acc_val;
        // position open notional
        let pos_open_notional = safe_mul_i80f48(
            I80F48::from_num(cmp::max(
                (oo_info.pos_size + oo_info.coin_on_bids as i64).abs(),
                (oo_info.pos_size - oo_info.coin_on_asks as i64).abs(),
            )),
            mark,
        )
        .floor()
        .to_num::<i64>();

        if pos_open_notional.is_positive() {
            has_open_pos_notional = true;
        }
        // has_open_pos_notional = has_open_pos_notional.safe_add(pos_open_notional)?;

        // pIMF or pMMF
        let base_imf = perp_markets[index].base_imf;
        let mf: u16 = match fraction_type {
            FractionType::Maintenance => base_imf.safe_div(2u16).unwrap(),
            FractionType::Initial => base_imf,
            FractionType::Cancel => {
                base_imf.safe_mul(5u16).unwrap().safe_div(8u16).unwrap()
            }
        };
        mf_vec.push(mf);
        pos_open_notional_vec.push(pos_open_notional);

        total_realized_pnl =
            total_realized_pnl.safe_add(oo_info.realized_pnl).unwrap();
    }

    Ok((
        total_acc_value,
        has_open_pos_notional,
        total_realized_pnl,
        mf_vec,
        pos_open_notional_vec,
    ))
}

fn get_spot_borrows(
    returns_imf: bool,
    max_cols: usize,
    col_arr: &[WrappedI80F48; 25],
    col_info_arr: &[CollateralInfo; 25],
    cache: &Ref<Cache>,
    total_realized_pnl: i64,
) -> Result<(bool, Vec<u16>, Vec<i64>), ErrorCode> {
    // for omf
    let mut has_open_pos_notional = false;

    // for imf or mmf
    let mut mf_vec = Vec::new();
    let mut pos_open_notional_vec = Vec::new();

    // loop through negative margin collateral
    for (dep_index, col_info) in col_info_arr.iter().enumerate() {
        if dep_index >= max_cols {
            break;
        }

        let bor_info = &cache.borrow_cache[dep_index];
        let mut dep: I80F48 = calc_actual_collateral(
            col_arr[dep_index].into(),
            bor_info.supply_multiplier.into(),
            bor_info.borrow_multiplier.into(),
        )
        .unwrap();
        // if collateral is USD, add the pos_realized_pnl
        if dep_index == 0 {
            dep += I80F48::from_num(total_realized_pnl);
        }

        if dep >= 0u64 {
            continue;
        }

        // get oracle price
        let oracle_cache = get_oracle(cache, &col_info.oracle_symbol).unwrap();
        let oracle_price: I80F48 = oracle_cache.price.into();

        // get position notional
        let pos_notional =
            safe_mul_i80f48(oracle_price, -dep).floor().to_num::<i64>();

        // add it to total open pos notional
        if pos_notional.is_positive() {
            has_open_pos_notional = true;
        }
        // has_open_pos_notional = has_open_pos_notional.safe_add(pos_notional)?;

        // pIMF or pMMF
        let mf: u16 = if returns_imf {
            (SPOT_INITIAL_MARGIN_REQ as u32 / col_info.weight as u32) as u16
                - 1000u16
        } else {
            (SPOT_MAINT_MARGIN_REQ as u32 / col_info.weight as u32) as u16
                - 1000u16
        };
        mf_vec.push(mf);
        pos_open_notional_vec.push(pos_notional);
    }
    Ok((has_open_pos_notional, mf_vec, pos_open_notional_vec))
}

fn calc_weighted_sum(
    factor: Vec<u16>,
    weights: Vec<i64>,
) -> Result<i64, ErrorCode> {
    let mut numerator = 0i64;

    for (i, &factor) in factor.iter().enumerate() {
        numerator += (factor as i64).safe_mul(weights[i]).unwrap();
        //println!("factor: {}, weight: {}", factor, weights[i]);
    }

    Ok(numerator)
}

fn calc_acc_val(
    collateral: i64,
    smol_mark_price: I80F48, // in smol usd per smol asset
    pos_size: i64,
    native_pc_total: i64,
    realized_pnl: i64,
    current_funding_index: i128,
    market_funding_index: i128,
    coin_decimals: u32,
) -> Result<i64, ErrorCode> {
    if pos_size == 0 {
        // msg!("collateral {}", collateral);
        // msg!("realized_pnl {}", realized_pnl);
        return Ok(collateral + realized_pnl);
    }

    let funding_diff = market_funding_index
        .safe_sub(current_funding_index)
        .unwrap();
    let unrealized_funding = pos_size
        .safe_mul(-funding_diff)
        .unwrap()
        .safe_div(10i64.pow(coin_decimals))
        .unwrap();

    let unrealized_pnl = if pos_size > 0 {
        let pos = safe_mul_i80f48(I80F48::from_num(pos_size), smol_mark_price)
            .floor()
            .to_num::<i64>();
        let bor = -native_pc_total;
        pos.safe_sub(bor).unwrap()
    } else {
        let pos = native_pc_total;
        let bor = safe_mul_i80f48(I80F48::from_num(-pos_size), smol_mark_price)
            .floor()
            .to_num::<i64>();
        pos.safe_sub(bor).unwrap()
    };
    Ok(collateral + realized_pnl + unrealized_pnl + unrealized_funding)
}

pub fn get_actual_collateral_vec(
    margin: &Margin,
    state: &Ref<State>,
    cache: &Ref<Cache>,
    is_weighted: bool,
) -> Result<Vec<I80F48>, ErrorCode> {
    let mut vec = Vec::with_capacity({ margin.collateral }.len());

    let max_col = state.total_collaterals;
    for (i, _v) in { margin.collateral }.iter().enumerate() {
        if i >= max_col as usize {
            break;
        }

        let info = &state.collaterals[i];
        let borrow = &cache.borrow_cache[i];

        if info.is_empty() {
            continue;
        }

        let v: I80F48 = get_actual_collateral(
            i,
            margin,
            borrow.supply_multiplier.into(),
            borrow.borrow_multiplier.into(),
        )
        .unwrap();

        let oracle_cache = get_oracle(cache, &info.oracle_symbol).unwrap();
        let price: I80F48 = oracle_cache.price.into();

        // Price is only weighted when collateral is non-negative.
        let weighted_price = match is_weighted && v >= 0u64 {
            true => safe_mul_i80f48(
                price,
                I80F48::from_num(info.weight as f64 / 1000.0),
            ),
            false => price,
        };
        vec.push(safe_mul_i80f48(weighted_price, v));
    }

    Ok(vec)
}

pub fn get_actual_collateral(
    index: usize,
    margin: &Margin,
    supply_multiplier: I80F48,
    borrow_multiplier: I80F48,
) -> Result<I80F48, ErrorCode> {
    let initial_col: I80F48 = margin.collateral[index].into();
    calc_actual_collateral(initial_col, supply_multiplier, borrow_multiplier)
}

pub fn calc_actual_collateral(
    initial_col: I80F48,
    supply_multiplier: I80F48,
    borrow_multiplier: I80F48,
) -> Result<I80F48, ErrorCode> {
    if initial_col > I80F48::ZERO {
        Ok(safe_mul_i80f48(initial_col, supply_multiplier))
    } else {
        Ok(safe_mul_i80f48(initial_col, borrow_multiplier))
    }
}

pub fn largest_open_order(
    cache: &Cache,
    control: &Control,
) -> Result<Option<usize>, ErrorCode> {
    let open_orders: Vec<I80F48> = control
        .open_orders_agg
        .iter()
        .zip(cache.marks)
        .map(|(order, mark)| {
            safe_mul_i80f48(
                I80F48::from_num(order.coin_on_asks.max(order.coin_on_bids)),
                mark.price.into(),
            )
        })
        .collect();

    let open_orders = open_orders.iter().enumerate();

    let open_order: Option<(usize, &I80F48)> =
        match open_orders.max_by_key(|a| a.1) {
            Some(x) => {
                if x.1.is_zero() {
                    None
                } else {
                    Some(x)
                }
            }
            None => return Err(ErrorCode::NoPositions),
        };

    if open_order == None || open_order.unwrap().1.is_zero() {
        return Ok(None);
    }

    Ok(Some(open_order.unwrap().0))
}

pub fn has_open_orders(
    cache: &Cache,
    control: &Control,
) -> Result<bool, ErrorCode> {
    let result = largest_open_order(cache, control)?;
    Ok(result.is_some())
}
