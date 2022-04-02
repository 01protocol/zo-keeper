use fixed::types::I80F48;

use std::cell::Ref;

use zo_abi::{
    Cache, Control, FractionType, Margin, PerpType, State, MAX_COLLATERALS,
    MAX_MARKETS, SPOT_INITIAL_MARGIN_REQ, SPOT_MAINT_MARGIN_REQ,
};

use crate::liquidator::{error::ErrorCode, math::*, utils::*};

#[derive(Clone, Copy)]
enum MfReturnOption {
    Mf,
    Imf,
    Mmf,
    Omf,
    Cmf,
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

/*
 * ###############################################################
 * ####################  START OF OVERHAUL  ######################
 * ###############################################################
 * All values are reported in small units, i.e. 1,000,000 = 1 USD.
 * This information available in the state collateral info.
 * All values are also in I80F48 format
 * to prevent floating point errors.
 * Functions are meant to be stand-alone and easy to test.
 * Also note that we don't deal with the total position open notional.
 * It appears the same way in each formula, so it is redundant to include it.
*/

/// Make a vector of positions for the given margin.
/// Each entry is denominated in smol assets.
/// Does not include pnl or open orders.
/// Mostly a helper function to interface margin and math.
fn get_position_vector(
    margin: &Margin,
    control: &Control,
) -> [I80F48; MAX_COLLATERALS + MAX_MARKETS] {
    let mut position = [I80F48::ZERO; MAX_COLLATERALS + MAX_MARKETS];

    for i in 0..MAX_COLLATERALS {
        position[i] = margin.collateral[i].into(); // In smol
    }

    for i in 0..MAX_MARKETS {
        position[i + MAX_COLLATERALS] =
            I80F48::from_num(control.open_orders_agg[i].pos_size);
    }

    position
}

fn get_position_open_vector(
    margin: &Margin,
    control: &Control,
) -> [I80F48; MAX_COLLATERALS + MAX_MARKETS] {
    let mut position = get_position_vector(margin, control);

    for (i, info) in control.open_orders_agg.iter().enumerate() {
        position[i + MAX_COLLATERALS] = I80F48::from_num(
            { info.pos_size }
                .safe_add(info.coin_on_bids as i64)
                .unwrap()
                .abs(),
        )
        .max(I80F48::from_num(
            { info.pos_size }
                .safe_sub(info.coin_on_asks as i64)
                .unwrap()
                .abs(),
        ));
    }
    position
}

/// Analogous vector for the state.
pub fn get_price_vector(
    state: &State,
    cache: &Cache,
    position: &[I80F48; MAX_COLLATERALS + MAX_MARKETS], // Needed to determine interest rates
) -> [I80F48; MAX_COLLATERALS + MAX_MARKETS] {
    // In sUSD/sAsset
    let mut price = [I80F48::ZERO; MAX_COLLATERALS + MAX_MARKETS];

    for i in 0..state.total_collaterals {
        let i = i as usize;
        let adjustment: I80F48 = if position[i].is_negative() {
            cache.borrow_cache[i].borrow_multiplier.into()
        } else {
            cache.borrow_cache[i].supply_multiplier.into()
        };

        let unadjusted_price =
            get_oracle(cache, &state.collaterals[i].oracle_symbol)
                .unwrap()
                .price
                .into();

        price[i] = safe_mul_i80f48(unadjusted_price, adjustment);
    }

    for i in 0..state.total_markets {
        let i = i as usize;
        match state.perp_markets[i].perp_type {
            PerpType::Future => {
                price[i + MAX_COLLATERALS] =
                    get_oracle(cache, &state.perp_markets[i].oracle_symbol)
                        .unwrap()
                        .price
                        .into();
            }
            PerpType::Square => {
                price[i + MAX_COLLATERALS] = cache.marks[i].price.into();
            }
            _ => {
                println!("Not implemented bruh");
            }
        }
    }

    price
}

pub fn get_pnl_vectors(
    control: &Control,
    state: &State,
    cache: &Cache,
    funding_cache: &[I80F48; MAX_MARKETS], // In smol for the asset
) -> (
    [I80F48; MAX_COLLATERALS + MAX_MARKETS],
    [I80F48; MAX_COLLATERALS + MAX_MARKETS],
) {
    let mut unrealized_pnls = [I80F48::ZERO; MAX_COLLATERALS + MAX_MARKETS];
    let mut realized_pnls = [I80F48::ZERO; MAX_COLLATERALS + MAX_MARKETS];

    for (i, &info) in control.open_orders_agg.iter().enumerate() {
        if info.pos_size == 0 {
            continue;
        }
        // Realized pnl calcs
        let funding_diff = I80F48::from_num(info.funding_index)
            .unwrapped_sub(funding_cache[i]);
        let unrealized_funding =
            safe_mul_i80f48(funding_diff, I80F48::from_num(info.pos_size))
                .unwrapped_div(I80F48::from_num(
                    10u64.pow(state.perp_markets[i].asset_decimals as u32),
                )); // In smol asset

        // Unrealized pnl calcs
        let price = match state.perp_markets[i].perp_type {
            PerpType::Future => {
                get_oracle(cache, &state.perp_markets[i].oracle_symbol)
                    .unwrap()
                    .price
                    .into()
            }
            PerpType::Square => cache.marks[i].price.into(),
            _ => {
                println!("Not implemented bruh");
                I80F48::ZERO
            }
        };

        let unrealized_pnl =
            safe_mul_i80f48(I80F48::from_num(info.pos_size), price)
                .unwrapped_add(I80F48::from_num(info.native_pc_total));

        unrealized_pnls[i + MAX_COLLATERALS] = unrealized_pnl;

        realized_pnls[i + MAX_COLLATERALS] =
            unrealized_funding + I80F48::from_num(info.realized_pnl);
    }
    (realized_pnls, unrealized_pnls)
}
/// Get weight vector
pub fn get_base_weight_vector(
    state: &State,
) -> [I80F48; MAX_COLLATERALS + MAX_MARKETS] {
    let mut weight = [I80F48::ZERO; MAX_COLLATERALS + MAX_MARKETS];

    for i in 0..state.total_collaterals as usize {
        weight[i] = I80F48::from_num(state.collaterals[i].weight)
            .unwrapped_div(I80F48::from_num(1000u32));
    }

    for i in 0..state.total_markets as usize {
        weight[i + MAX_COLLATERALS] =
            I80F48::from_num(state.perp_markets[i].base_imf)
                .unwrapped_div(I80F48::from_num(1000u32));
    }

    weight
}

fn weight_conversion(
    return_type: MfReturnOption,
    position: &I80F48,
    base_weight: &I80F48,
    is_spot: bool,
) -> I80F48 {
    match return_type {
        MfReturnOption::Mf | MfReturnOption::Omf => {
            if is_spot && !position.is_negative() {
                base_weight.clone()
            } else if is_spot {
                I80F48::ONE
            } else {
                I80F48::ZERO
            }
        }
        MfReturnOption::Imf => {
            if is_spot && !position.is_negative() {
                I80F48::ZERO
            } else if is_spot {
                -(I80F48::from_num(SPOT_INITIAL_MARGIN_REQ)
                    / I80F48::from_num(1_000_000))
                .unwrapped_div(base_weight.clone())
                .unwrapped_sub(I80F48::ONE)
            } else {
                let sign = if position.is_negative() { -1 } else { 1 };

                sign * base_weight.clone()
            }
        }
        MfReturnOption::Cmf => {
            if is_spot && !position.is_negative() {
                I80F48::ZERO
            } else if is_spot {
                -(I80F48::from_num(SPOT_INITIAL_MARGIN_REQ)
                    / I80F48::from_num(1_000_000u64))
                .unwrapped_div(base_weight.clone())
                .unwrapped_sub(I80F48::ONE)
            } else {
                let sign = if position.is_negative() { -1 } else { 1 };

                sign * safe_mul_i80f48(
                    I80F48::from_str_binary("0.101").unwrap(),
                    base_weight.clone(),
                )
            }
        }
        MfReturnOption::Mmf => {
            if is_spot && !position.is_negative() {
                I80F48::ZERO
            } else if is_spot {
                -(I80F48::from_num(SPOT_MAINT_MARGIN_REQ)
                    / I80F48::from_num(1_000_000))
                .unwrapped_div(base_weight.clone())
                .unwrapped_sub(I80F48::ONE)
            } else {
                let sign = if position.is_negative() { -1 } else { 1 };

                sign * safe_mul_i80f48(
                    I80F48::from_str_binary("0.1").unwrap(),
                    base_weight.clone(),
                )
            }
        }
    }
}

#[allow(dead_code)]
fn get_weight_vector(
    return_type: MfReturnOption,
    position: &[I80F48; MAX_COLLATERALS + MAX_MARKETS],
    base_weight: &[I80F48; MAX_COLLATERALS + MAX_MARKETS],
) -> [I80F48; MAX_COLLATERALS + MAX_MARKETS] {
    base_weight
        .iter()
        .enumerate()
        .zip(position.iter())
        .map(|((i, base), pos)| {
            weight_conversion(return_type, pos, base, i < MAX_COLLATERALS)
        })
        .collect::<Vec<I80F48>>()
        .try_into()
        .unwrap()
}

fn get_mf(
    mf: MfReturnOption,
    position: &[I80F48; MAX_COLLATERALS + MAX_MARKETS],
    prices: &[I80F48; MAX_COLLATERALS + MAX_MARKETS],
    realized_pnl: &[I80F48; MAX_COLLATERALS + MAX_MARKETS],
    unrealized_pnl: &[I80F48; MAX_COLLATERALS + MAX_MARKETS],
    base_weight: &[I80F48; MAX_COLLATERALS + MAX_MARKETS],
) -> I80F48 {
    let mut mf_value = I80F48::ZERO;

    let total_realized_pnl: I80F48 = realized_pnl.iter().sum();
    let total_unrealized_pnl: I80F48 = unrealized_pnl.iter().sum();

    for i in 0..(MAX_COLLATERALS + MAX_MARKETS) {
        let weight = weight_conversion(
            mf,
            &position[i],
            &base_weight[i],
            i < MAX_COLLATERALS,
        );
        let weighted_price = safe_mul_i80f48(prices[i], weight);

        if i == 0 {
            mf_value = safe_add_i80f48(
                mf_value,
                safe_mul_i80f48(total_realized_pnl, weight), // Already in big at t=T
            );
        }

        mf_value = safe_add_i80f48(
            mf_value,
            safe_mul_i80f48(position[i], weighted_price),
        );
    }
    let pos_unrealized = match mf {
        MfReturnOption::Mf => total_unrealized_pnl,
        MfReturnOption::Omf => total_unrealized_pnl.min(I80F48::ZERO),
        _ => I80F48::ZERO,
    };

    mf_value + pos_unrealized
}

fn get_mf_wrapped(
    mf: MfReturnOption,
    margin: &Margin,
    control: &Control,
    state: &State,
    cache: &Cache,
) -> I80F48 {
    let position_vector = match mf {
        MfReturnOption::Imf => get_position_open_vector(margin, control),
        MfReturnOption::Cmf => get_position_open_vector(margin, control),
        _ => get_position_vector(margin, control),
    };

    let price_vector = get_price_vector(state, cache, &position_vector);

    let weight_vector = get_base_weight_vector(state);

    let funding_cache: [I80F48; MAX_MARKETS] = { cache.funding_cache }
        .iter()
        .map(|x| I80F48::from_num(*x)) //  i128 to I80 might not be ideal.
        // Think if dividing here instead of in pnl and using pos_size in pnl
        .collect::<Vec<I80F48>>()
        .try_into()
        .unwrap(); // This is a bruh moment

    let (realized_pnl, unrealized_pnl) =
        get_pnl_vectors(control, state, cache, &funding_cache);

    get_mf(
        mf,
        &position_vector,
        &price_vector,
        &realized_pnl,
        &unrealized_pnl,
        &weight_vector,
    )
}

pub fn check_mf(
    check: FractionType,
    margin: &Margin,
    control: &Control,
    state: &State,
    cache: &Cache,
    tolerance: I80F48, // for making sure the account is liquidatable, should be less than 1.0
) -> bool {
    let position_vector = match check {
        FractionType::Initial | FractionType::Cancel => {
            get_position_open_vector(margin, control)
        }
        _ => get_position_vector(margin, control),
    };

    let price_vector = get_price_vector(state, cache, &position_vector);

    let weight_vector = get_base_weight_vector(state);

    let funding_cache: [I80F48; MAX_MARKETS] = { cache.funding_cache }
        .iter()
        .map(|x| I80F48::from_num(*x)) //  i128 to I80 might not be ideal.
        // Think if dividing here instead of in pnl and using pos_size in pnl
        .collect::<Vec<I80F48>>()
        .try_into()
        .unwrap(); // This is a bruh moment

    let (realized_pnl, unrealized_pnl) =
        get_pnl_vectors(control, state, cache, &funding_cache);

    match check {
        FractionType::Initial => {
            let omf = get_mf(
                MfReturnOption::Omf,
                &position_vector,
                &price_vector,
                &realized_pnl,
                &unrealized_pnl,
                &weight_vector,
            );
            let imf = get_mf(
                MfReturnOption::Imf,
                &position_vector,
                &price_vector,
                &realized_pnl,
                &unrealized_pnl,
                &weight_vector,
            );
            omf >= safe_mul_i80f48(imf, tolerance)
        }
        FractionType::Cancel => {
            let omf = get_mf(
                MfReturnOption::Omf,
                &position_vector,
                &price_vector,
                &realized_pnl,
                &unrealized_pnl,
                &weight_vector,
            );
            let cmf = get_mf(
                MfReturnOption::Cmf,
                &position_vector,
                &price_vector,
                &realized_pnl,
                &unrealized_pnl,
                &weight_vector,
            );
            omf >= safe_mul_i80f48(cmf, tolerance)
        }
        FractionType::Maintenance => {
            let mf = get_mf(
                MfReturnOption::Mf,
                &position_vector,
                &price_vector,
                &realized_pnl,
                &unrealized_pnl,
                &weight_vector,
            );
            let mmf = get_mf(
                MfReturnOption::Mmf,
                &position_vector,
                &price_vector,
                &realized_pnl,
                &unrealized_pnl,
                &weight_vector,
            );
            mf >= safe_mul_i80f48(mmf, tolerance)
        }
    }
}

pub fn get_total_account_value(
    margin: &Margin,
    control: &Control,
    state: &State,
    cache: &Cache,
) -> I80F48 {
    get_mf_wrapped(MfReturnOption::Mf, margin, control, state, cache)
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

/// The estimate of how much asset will be liquidated in spot.
/// This is a negative number (we are lending the i'th asset).
/// We want to buy this asset afterwards (with USDC), so we want
/// to denominate the result of this function is sUSD.
pub fn estimate_spot_liquidation_size(
    margin: &Margin,
    control: &Control,
    state: &State,
    cache: &Cache,
    asset_index: usize, // The asset index
    quote_index: usize,
) -> Option<I80F48> {
    let mut position = get_position_open_vector(margin, control);

    let funding_cache: [I80F48; MAX_MARKETS] = { cache.funding_cache }
        .iter()
        .map(|x| I80F48::from_num(*x)) //  i128 to I80 might not be ideal.
        // Think if dividing here instead of in pnl and using pos_size in pnl
        .collect::<Vec<I80F48>>()
        .try_into()
        .unwrap(); // This is a bruh moment

    let price_vector = get_price_vector(state, cache, &position);

    let (realized_pnl, unrealized_pnl) =
        get_pnl_vectors(control, state, cache, &funding_cache);

    let total_realized_pnl =
        realized_pnl.iter().sum::<I80F48>() / price_vector[0];

    position[0] += total_realized_pnl;

    let weight_vector = get_base_weight_vector(state);

    let imf_weight =
        get_weight_vector(MfReturnOption::Imf, &position, &weight_vector);
    let omf_weight =
        get_weight_vector(MfReturnOption::Omf, &position, &weight_vector);

    let quote_fee = I80F48::from_num(state.collaterals[quote_index].liq_fee)
        / I80F48::from_num(1000u32);
    let asset_fee = I80F48::from_num(state.collaterals[asset_index].liq_fee)
        / I80F48::from_num(1000u32);
    let liq_fee = (I80F48::ONE + asset_fee) / (I80F48::ONE - quote_fee);

    let asset_price: I80F48 =
        get_oracle(cache, &state.collaterals[asset_index].oracle_symbol)
            .unwrap()
            .price
            .into();

    let denom: I80F48 = asset_price
        * (omf_weight[quote_index] * liq_fee
            - omf_weight[asset_index]
            - imf_weight[quote_index]
            + imf_weight[asset_index]);

    if denom.abs() < I80F48::from_num(0.0001f64) {
        // denom in smol so....
        return None;
    }

    let mut numerator = unrealized_pnl.iter().sum::<I80F48>().min(I80F48::ZERO);

    for i in 0..(MAX_MARKETS + MAX_COLLATERALS) {
        numerator +=
            position[i] * price_vector[i] * (omf_weight[i] - imf_weight[i]);
    }

    let amount = numerator.saturating_div(denom);

    if amount.is_positive() {
        let usdc_amount = amount * price_vector[asset_index];
        Some(
            usdc_amount
                .min(
                    -I80F48::from(margin.collateral[asset_index])
                        * price_vector[asset_index],
                )
                .min(
                    I80F48::from(margin.collateral[quote_index])
                        * price_vector[quote_index],
                ),
        )
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anchor_lang::prelude::Pubkey;
    use solana_client::rpc_client::RpcClient;
    use std::str::FromStr;

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn test_get_weights_imf() {
        let mut position = [I80F48::ZERO; MAX_COLLATERALS + MAX_MARKETS];
        position[0] = I80F48::from_num(450_000_000u64); // 450 USDC
        position[1] = I80F48::from_num(-1_000_000_000i64); // 1 SOL

        let mut weights = [I80F48::ZERO; MAX_COLLATERALS + MAX_MARKETS];
        weights[0] = I80F48::ONE;
        weights[1] = I80F48::from_num(0.9f64);
        weights[MAX_COLLATERALS] = I80F48::from_num(0.9f64);

        let adjusted_weights =
            get_weight_vector(MfReturnOption::Imf, &position, &weights);

        let mut true_weights = [I80F48::ZERO; MAX_COLLATERALS + MAX_MARKETS];
        true_weights[0] = I80F48::ZERO;
        true_weights[1] = -I80F48::from_num(1.1f64)
            .unwrapped_div(I80F48::from_num(0.9f64))
            + I80F48::ONE;
        true_weights[MAX_COLLATERALS] = I80F48::from_num(0.9f64);

        for i in 0..(MAX_COLLATERALS + MAX_MARKETS) {
            assert!(true_weights[i].eq(&adjusted_weights[i]));
        }
    }

    #[test]
    fn test_get_position_vector() {
        let rpc_client =
            RpcClient::new("https://solana-api.syndica.io/access-token/3IAUwhDwhzjX2Fg5s9HLYfjyoAfSz80hYyOPACaVZhJsqo4HsjIzUr74aN01F8QQ/rpc".to_string());

        let margins =
            load_program_accounts::<Margin>(&rpc_client, &zo_abi::ID).unwrap();
        let controls =
            load_program_accounts::<Control>(&rpc_client, &zo_abi::ID).unwrap();

        let mut test_margin: Option<Margin> = None;
        for (_key, margin) in margins.iter() {
            if margin.authority.eq(&Pubkey::from_str(
                "AL8JFS4gjaQx89f9j8wtaNJgV76K8bw1ugvNtgvhgAnb",
            )
            .unwrap())
            {
                test_margin = Some(margin.clone());
                break;
            }
        }

        assert!(test_margin.is_some());

        let mut test_control: Option<Control> = None;
        for (key, control) in controls.iter() {
            if key.eq(&test_margin.unwrap().control) {
                test_control = Some(control.clone());
                break;
            }
        }

        assert!(test_control.is_some());

        let position =
            get_position_vector(&test_margin.unwrap(), &test_control.unwrap());
        let mut true_position = [I80F48::ZERO; MAX_COLLATERALS + MAX_MARKETS];

        true_position[0] = I80F48::from_num(1.604205999948498f64);
        true_position[MAX_COLLATERALS] = I80F48::from_num(140_000_000u64); // 1 SOL

        for i in 0..(MAX_COLLATERALS + MAX_MARKETS) {
            println!("{} expected {} got {}", i, true_position[i], position[i]);
        }
    }

    #[test]
    fn test_get_account_value() {
        let rpc_client =
            RpcClient::new("https://solana-api.syndica.io/access-token/3IAUwhDwhzjX2Fg5s9HLYfjyoAfSz80hYyOPACaVZhJsqo4HsjIzUr74aN01F8QQ/rpc".to_string());

        let state: State =
            load_program_accounts::<State>(&rpc_client, &zo_abi::ID).unwrap()
                [0]
            .1;

        let cache: Cache =
            load_program_accounts::<Cache>(&rpc_client, &&zo_abi::ID).unwrap()
                [0]
            .1;

        let margins =
            load_program_accounts::<Margin>(&rpc_client, &zo_abi::ID).unwrap();
        let controls =
            load_program_accounts::<Control>(&rpc_client, &zo_abi::ID).unwrap();

        let mut test_margin: Option<Margin> = None;
        for (_key, margin) in margins.iter() {
            if margin.authority.eq(&Pubkey::from_str(
                "AL8JFS4gjaQx89f9j8wtaNJgV76K8bw1ugvNtgvhgAnb",
            )
            .unwrap())
            {
                test_margin = Some(margin.clone());
                break;
            }
        }

        assert!(test_margin.is_some());

        let mut test_control: Option<Control> = None;
        for (key, control) in controls.iter() {
            if key.eq(&test_margin.unwrap().control) {
                test_control = Some(control.clone());
                break;
            }
        }

        assert!(test_control.is_some());

        let mf = get_mf_wrapped(
            MfReturnOption::Mf,
            &test_margin.unwrap(),
            &test_control.unwrap(),
            &state,
            &cache,
        );
        println!("{}", mf)
    }

    #[test]
    fn test_get_mmf() {
        let rpc_client =
            RpcClient::new("https://solana-api.syndica.io/access-token/3IAUwhDwhzjX2Fg5s9HLYfjyoAfSz80hYyOPACaVZhJsqo4HsjIzUr74aN01F8QQ/rpc".to_string());

        let state: State =
            load_program_accounts::<State>(&rpc_client, &zo_abi::ID).unwrap()
                [0]
            .1;

        let cache: Cache =
            load_program_accounts::<Cache>(&rpc_client, &&zo_abi::ID).unwrap()
                [0]
            .1;

        let margins =
            load_program_accounts::<Margin>(&rpc_client, &zo_abi::ID).unwrap();
        let controls =
            load_program_accounts::<Control>(&rpc_client, &zo_abi::ID).unwrap();

        let mut test_margin: Option<Margin> = None;
        for (_key, margin) in margins.iter() {
            if margin.authority.eq(&Pubkey::from_str(
                "AL8JFS4gjaQx89f9j8wtaNJgV76K8bw1ugvNtgvhgAnb",
            )
            .unwrap())
            {
                test_margin = Some(margin.clone());
                break;
            }
        }

        assert!(test_margin.is_some());

        let mut test_control: Option<Control> = None;
        for (key, control) in controls.iter() {
            if key.eq(&test_margin.unwrap().control) {
                test_control = Some(control.clone());
                break;
            }
        }

        assert!(test_control.is_some());

        let mmf = get_mf_wrapped(
            MfReturnOption::Mmf,
            &test_margin.unwrap(),
            &test_control.unwrap(),
            &state,
            &cache,
        );
        println!("{}", mmf)
    }

    #[test]
    fn test_get_imf() {
        let rpc_client =
            RpcClient::new("https://solana-api.syndica.io/access-token/3IAUwhDwhzjX2Fg5s9HLYfjyoAfSz80hYyOPACaVZhJsqo4HsjIzUr74aN01F8QQ/rpc".to_string());

        let state: State =
            load_program_accounts::<State>(&rpc_client, &zo_abi::ID).unwrap()
                [0]
            .1;

        let cache: Cache =
            load_program_accounts::<Cache>(&rpc_client, &&zo_abi::ID).unwrap()
                [0]
            .1;

        let margins =
            load_program_accounts::<Margin>(&rpc_client, &zo_abi::ID).unwrap();
        let controls =
            load_program_accounts::<Control>(&rpc_client, &zo_abi::ID).unwrap();

        let mut test_margin: Option<Margin> = None;
        for (_key, margin) in margins.iter() {
            if margin.authority.eq(&Pubkey::from_str(
                "AL8JFS4gjaQx89f9j8wtaNJgV76K8bw1ugvNtgvhgAnb",
            )
            .unwrap())
            {
                test_margin = Some(margin.clone());
                break;
            }
        }

        assert!(test_margin.is_some());

        let mut test_control: Option<Control> = None;
        for (key, control) in controls.iter() {
            if key.eq(&test_margin.unwrap().control) {
                test_control = Some(control.clone());
                break;
            }
        }

        assert!(test_control.is_some());

        let imf = get_mf_wrapped(
            MfReturnOption::Imf,
            &test_margin.unwrap(),
            &test_control.unwrap(),
            &state,
            &cache,
        );
        println!("{}", imf);
    }

    #[test]
    fn test_imf_cmf() {
        let rpc_client =
            RpcClient::new("https://solana-api.syndica.io/access-token/3IAUwhDwhzjX2Fg5s9HLYfjyoAfSz80hYyOPACaVZhJsqo4HsjIzUr74aN01F8QQ/rpc".to_string());

        let state: State =
            load_program_accounts::<State>(&rpc_client, &zo_abi::ID).unwrap()
                [0]
            .1;

        let cache: Cache =
            load_program_accounts::<Cache>(&rpc_client, &&zo_abi::ID).unwrap()
                [0]
            .1;

        let margins =
            load_program_accounts::<Margin>(&rpc_client, &zo_abi::ID).unwrap();
        let controls =
            load_program_accounts::<Control>(&rpc_client, &zo_abi::ID).unwrap();

        let mut test_margin: Option<Margin> = None;
        for (_key, margin) in margins.iter() {
            if margin.authority.eq(&Pubkey::from_str(
                "AL8JFS4gjaQx89f9j8wtaNJgV76K8bw1ugvNtgvhgAnb",
            )
            .unwrap())
            {
                test_margin = Some(margin.clone());
                break;
            }
        }

        assert!(test_margin.is_some());

        let mut test_control: Option<Control> = None;
        for (key, control) in controls.iter() {
            if key.eq(&test_margin.unwrap().control) {
                test_control = Some(control.clone());
                break;
            }
        }

        assert!(test_control.is_some());

        let cmf = get_mf_wrapped(
            MfReturnOption::Cmf,
            &test_margin.unwrap(),
            &test_control.unwrap(),
            &state,
            &cache,
        );

        let imf = get_mf_wrapped(
            MfReturnOption::Imf,
            &test_margin.unwrap(),
            &test_control.unwrap(),
            &state,
            &cache,
        );

        assert!(
            cmf.unwrapped_sub(safe_mul_i80f48(
                imf,
                I80F48::from_str_binary("0.101").unwrap()
            ))
            .abs()
                <= I80F48::from_str_binary("0.0001").unwrap()
        );
    }

    #[test]
    fn test_check_mf_maintenance() {
        let rpc_client =
            RpcClient::new("https://solana-api.syndica.io/access-token/3IAUwhDwhzjX2Fg5s9HLYfjyoAfSz80hYyOPACaVZhJsqo4HsjIzUr74aN01F8QQ/rpc".to_string());

        let state: State =
            load_program_accounts::<State>(&rpc_client, &zo_abi::ID).unwrap()
                [0]
            .1;

        let cache: Cache =
            load_program_accounts::<Cache>(&rpc_client, &&zo_abi::ID).unwrap()
                [0]
            .1;

        let margins =
            load_program_accounts::<Margin>(&rpc_client, &zo_abi::ID).unwrap();
        let controls =
            load_program_accounts::<Control>(&rpc_client, &zo_abi::ID).unwrap();

        let mut test_margin: Option<Margin> = None;
        for (_key, margin) in margins.iter() {
            if margin.authority.eq(&Pubkey::from_str(
                "53qyL9jgfsABQAsn3ZUSstd5fQv2Kqf1KeAMVgscmDBz",
            )
            .unwrap())
            {
                test_margin = Some(margin.clone());
                break;
            }
        }

        assert!(test_margin.is_some());

        let mut test_control: Option<Control> = None;
        for (key, control) in controls.iter() {
            if key.eq(&test_margin.unwrap().control) {
                test_control = Some(control.clone());
                break;
            }
        }

        assert!(test_control.is_some());

        let is_ok = check_mf(
            FractionType::Maintenance,
            &test_margin.unwrap(),
            &test_control.unwrap(),
            &state,
            &cache,
            I80F48::from_num(0.99f64),
        );
        // The liquidator is ok
        assert!(is_ok);
    }

    #[test]
    fn test_check_mf_cancel() {
        let rpc_client =
            RpcClient::new("https://solana-api.syndica.io/access-token/3IAUwhDwhzjX2Fg5s9HLYfjyoAfSz80hYyOPACaVZhJsqo4HsjIzUr74aN01F8QQ/rpc".to_string());

        let state: State =
            load_program_accounts::<State>(&rpc_client, &zo_abi::ID).unwrap()
                [0]
            .1;

        let cache: Cache =
            load_program_accounts::<Cache>(&rpc_client, &&zo_abi::ID).unwrap()
                [0]
            .1;

        let margins =
            load_program_accounts::<Margin>(&rpc_client, &zo_abi::ID).unwrap();
        let controls =
            load_program_accounts::<Control>(&rpc_client, &zo_abi::ID).unwrap();

        let mut test_margin: Option<Margin> = None;
        for (_key, margin) in margins.iter() {
            if margin.authority.eq(&Pubkey::from_str(
                "53qyL9jgfsABQAsn3ZUSstd5fQv2Kqf1KeAMVgscmDBz",
            )
            .unwrap())
            {
                test_margin = Some(margin.clone());
                break;
            }
        }

        assert!(test_margin.is_some());

        let mut test_control: Option<Control> = None;
        for (key, control) in controls.iter() {
            if key.eq(&test_margin.unwrap().control) {
                test_control = Some(control.clone());
                break;
            }
        }

        assert!(test_control.is_some());

        let is_ok = check_mf(
            FractionType::Cancel,
            &test_margin.unwrap(),
            &test_control.unwrap(),
            &state,
            &cache,
            I80F48::from_num(0.99f64),
        );
        assert!(is_ok);
    }

    #[test]
    fn test_check_mf_initial() {
        let rpc_client =
            RpcClient::new("https://solana-api.syndica.io/access-token/3IAUwhDwhzjX2Fg5s9HLYfjyoAfSz80hYyOPACaVZhJsqo4HsjIzUr74aN01F8QQ/rpc".to_string());

        let state: State =
            load_program_accounts::<State>(&rpc_client, &zo_abi::ID).unwrap()
                [0]
            .1;

        let cache: Cache =
            load_program_accounts::<Cache>(&rpc_client, &&zo_abi::ID).unwrap()
                [0]
            .1;

        let margins =
            load_program_accounts::<Margin>(&rpc_client, &zo_abi::ID).unwrap();
        let controls =
            load_program_accounts::<Control>(&rpc_client, &zo_abi::ID).unwrap();

        let mut test_margin: Option<Margin> = None;
        for (_key, margin) in margins.iter() {
            if margin.authority.eq(&Pubkey::from_str(
                "53qyL9jgfsABQAsn3ZUSstd5fQv2Kqf1KeAMVgscmDBz",
            )
            .unwrap())
            {
                test_margin = Some(margin.clone());
                break;
            }
        }

        assert!(test_margin.is_some());

        let mut test_control: Option<Control> = None;
        for (key, control) in controls.iter() {
            if key.eq(&test_margin.unwrap().control) {
                test_control = Some(control.clone());
                break;
            }
        }

        assert!(test_control.is_some());

        let is_ok = check_mf(
            FractionType::Initial,
            &test_margin.unwrap(),
            &test_control.unwrap(),
            &state,
            &cache,
            I80F48::from_num(0.99f64),
        );
        // The liquidator is ok
        assert!(is_ok);
    }

    #[test]
    fn test_get_base_weights() {
        let rpc_client =
            RpcClient::new("https://solana-api.syndica.io/access-token/3IAUwhDwhzjX2Fg5s9HLYfjyoAfSz80hYyOPACaVZhJsqo4HsjIzUr74aN01F8QQ/rpc".to_string());
        let state: State =
            load_program_accounts::<State>(&rpc_client, &zo_abi::ID).unwrap()
                [0]
            .1;

        let base = get_base_weight_vector(&state);
        let mut true_weights = [I80F48::ZERO; MAX_COLLATERALS + MAX_MARKETS];
        true_weights[0] = I80F48::ONE;
        true_weights[1] = I80F48::from_num(0.9f64);
        true_weights[2] = I80F48::from_num(0.9f64);
        true_weights[3] = I80F48::from_num(0.95f64);

        true_weights[MAX_COLLATERALS] = I80F48::from_num(0.1f64);
        true_weights[MAX_COLLATERALS + 1] = I80F48::from_num(0.1f64);
        true_weights[MAX_COLLATERALS + 2] = I80F48::from_num(0.1f64);

        for i in 0..(MAX_COLLATERALS + MAX_MARKETS) {
            println!("expected {} got {} at {}", true_weights[i], base[i], i);
            assert!(
                true_weights[i].unwrapped_sub(base[i]).abs()
                    < I80F48::from_num(0.00000001f64)
            );
        }
    }

    #[test]
    fn test_estimate_spot_liq_size() {
        let rpc_client =
            RpcClient::new("https://solana-api.syndica.io/access-token/3IAUwhDwhzjX2Fg5s9HLYfjyoAfSz80hYyOPACaVZhJsqo4HsjIzUr74aN01F8QQ/rpc".to_string());

        let state: State =
            load_program_accounts::<State>(&rpc_client, &zo_abi::ID).unwrap()
                [0]
            .1;

        let cache: Cache =
            load_program_accounts::<Cache>(&rpc_client, &&zo_abi::ID).unwrap()
                [0]
            .1;

        let margins =
            load_program_accounts::<Margin>(&rpc_client, &zo_abi::ID).unwrap();
        let controls =
            load_program_accounts::<Control>(&rpc_client, &zo_abi::ID).unwrap();

        let mut test_margin: Option<Margin> = None;
        for (_key, margin) in margins.iter() {
            if margin.authority.eq(&Pubkey::from_str(
                "53qyL9jgfsABQAsn3ZUSstd5fQv2Kqf1KeAMVgscmDBz",
            )
            .unwrap())
            {
                test_margin = Some(margin.clone());
                break;
            }
        }

        assert!(test_margin.is_some());

        let mut test_control: Option<Control> = None;
        for (key, control) in controls.iter() {
            if key.eq(&test_margin.unwrap().control) {
                test_control = Some(control.clone());
                break;
            }
        }

        assert!(test_control.is_some());

        let amount = estimate_spot_liquidation_size(
            &test_margin.unwrap(),
            &test_control.unwrap(),
            &state,
            &cache,
            2,
            0,
        );

        assert!(amount.is_none());

        let t2 = estimate_spot_liquidation_size(
            &test_margin.unwrap(),
            &test_control.unwrap(),
            &state,
            &cache,
            0,
            2,
        );

        assert!(t2.is_some());
    }

    #[test]
    fn test_estimate_spot_liq_size2() {
        let rpc_client =
            RpcClient::new("https://solana-api.syndica.io/access-token/3IAUwhDwhzjX2Fg5s9HLYfjyoAfSz80hYyOPACaVZhJsqo4HsjIzUr74aN01F8QQ/rpc".to_string());

        let state: State =
            load_program_accounts::<State>(&rpc_client, &zo_abi::ID).unwrap()
                [0]
            .1;

        let cache: Cache =
            load_program_accounts::<Cache>(&rpc_client, &&zo_abi::ID).unwrap()
                [0]
            .1;

        let margins =
            load_program_accounts::<Margin>(&rpc_client, &zo_abi::ID).unwrap();
        let controls =
            load_program_accounts::<Control>(&rpc_client, &zo_abi::ID).unwrap();

        let mut test_margin: Option<Margin> = None;
        for (_key, margin) in margins.iter() {
            if margin.authority.eq(&Pubkey::from_str(
                "53qyL9jgfsABQAsn3ZUSstd5fQv2Kqf1KeAMVgscmDBz",
            )
            .unwrap())
            {
                test_margin = Some(margin.clone());
                break;
            }
        }

        assert!(test_margin.is_some());

        let mut test_control: Option<Control> = None;
        for (key, control) in controls.iter() {
            if key.eq(&test_margin.unwrap().control) {
                test_control = Some(control.clone());
                break;
            }
        }

        assert!(test_control.is_some());

        let amount = estimate_spot_liquidation_size(
            &test_margin.unwrap(),
            &test_control.unwrap(),
            &state,
            &cache,
            1,
            0,
        );

        assert_eq!(amount.unwrap(), I80F48::from_num(382370000.0f64));

        let t2 = estimate_spot_liquidation_size(
            &test_margin.unwrap(),
            &test_control.unwrap(),
            &state,
            &cache,
            0,
            2,
        );

        assert!(t2.is_some());
    }

    #[test]
    fn test_check_mf_maintenance_main() {
        let rpc_client = RpcClient::new(
            "https://solana-api.syndica.io/access-token/3IAUwhDwhzjX2Fg5s9HLYfjyoAfSz80hYyOPACaVZhJsqo4HsjIzUr74aN01F8QQ/rpc".to_string(),
        );

        let state: State =
            load_program_accounts::<State>(&rpc_client, &zo_abi::ID).unwrap()
                [0]
            .1;

        let cache: Cache =
            load_program_accounts::<Cache>(&rpc_client, &zo_abi::ID).unwrap()
                [0]
            .1;
        let margins =
            load_program_accounts::<Margin>(&rpc_client, &zo_abi::ID).unwrap();
        let controls =
            load_program_accounts::<Control>(&rpc_client, &zo_abi::ID).unwrap();

        let mut test_margin: Option<Margin> = None;
        for (_key, margin) in margins.iter() {
            if margin.authority.eq(&Pubkey::from_str(
                "53qyL9jgfsABQAsn3ZUSstd5fQv2Kqf1KeAMVgscmDBz",
            )
            .unwrap())
            {
                test_margin = Some(margin.clone());
                break;
            }
        }

        assert!(test_margin.is_some());

        let mut test_control: Option<Control> = None;
        for (key, control) in controls.iter() {
            if key.eq(&test_margin.unwrap().control) {
                test_control = Some(control.clone());
                break;
            }
        }

        assert!(test_control.is_some());

        let mf = get_mf_wrapped(
            MfReturnOption::Mf,
            &test_margin.unwrap(),
            &test_control.unwrap(),
            &state,
            &cache,
        );

        let mmf = get_mf_wrapped(
            MfReturnOption::Mmf,
            &test_margin.unwrap(),
            &test_control.unwrap(),
            &state,
            &cache,
        );

        println!("{} {}", mf, mmf);
        let is_ok = check_mf(
            FractionType::Maintenance,
            &test_margin.unwrap(),
            &test_control.unwrap(),
            &state,
            &cache,
            I80F48::from_num(0.99f64),
        );
        // The liquidator is ok
        assert!(is_ok);
    }

    #[test]
    fn test_check_mf_maintenance_dev() {
        let rpc_client = RpcClient::new(
            "https://psytrbhymqlkfrhudd.dev.genesysgo.net:8899/".to_string(),
        );

        let state: State =
            load_program_accounts::<State>(&rpc_client, &zo_abi::ID).unwrap()
                [0]
            .1;

        let cache: Cache =
            load_program_accounts::<Cache>(&rpc_client, &zo_abi::ID).unwrap()
                [0]
            .1;
        let margins =
            load_program_accounts::<Margin>(&rpc_client, &zo_abi::ID).unwrap();
        let controls =
            load_program_accounts::<Control>(&rpc_client, &zo_abi::ID).unwrap();

        let mut test_margin: Option<Margin> = None;
        for (_key, margin) in margins.iter() {
            if margin.authority.eq(&Pubkey::from_str(
                "76FnoFsGx5axcYoB4Jzxyds2gGJmw7ddbVC7cL4n9fpa",
            )
            .unwrap())
            {
                test_margin = Some(margin.clone());
                break;
            }
        }

        assert!(test_margin.is_some());

        let mut test_control: Option<Control> = None;
        for (key, control) in controls.iter() {
            if key.eq(&test_margin.unwrap().control) {
                test_control = Some(control.clone());
                break;
            }
        }

        assert!(test_control.is_some());

        let mf = get_mf_wrapped(
            MfReturnOption::Mf,
            &test_margin.unwrap(),
            &test_control.unwrap(),
            &state,
            &cache,
        );

        let mmf = get_mf_wrapped(
            MfReturnOption::Mmf,
            &test_margin.unwrap(),
            &test_control.unwrap(),
            &state,
            &cache,
        );

        println!("{} {}", mf, mmf);
        let is_ok = check_mf(
            FractionType::Maintenance,
            &test_margin.unwrap(),
            &test_control.unwrap(),
            &state,
            &cache,
            I80F48::from_num(0.99f64),
        );
        // The liquidator is ok
        assert!(is_ok);
    }
}
