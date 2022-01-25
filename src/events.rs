// NOTE: Modified implementation of anchor's parser because anchor's impl has a few issues

use crate::{db, AppState, Error};
use anchor_client::anchor_lang::Event;
use futures::TryFutureExt;
use std::time::SystemTime;
use tracing::warn;
use zo_abi::events;

#[tracing::instrument(skip_all, level = "error")]
pub async fn process(
    st: &AppState,
    db: &mongodb::Database,
    ss: Vec<String>,
    sig: String,
) {
    let (rpnl, liq, bank, bal, swap, oracle) = parse(st, ss.iter(), sig);

    let on_err = |e| {
        let e = Error::from(e);
        warn!("{}", e);
    };
    let _ = futures::join!(
        db::RealizedPnl::update(db, &rpnl).map_err(on_err),
        db::Liquidation::update(db, &liq).map_err(on_err),
        db::Bankruptcy::update(db, &bank).map_err(on_err),
        db::BalanceChange::update(db, &bal).map_err(on_err),
        db::Swap::update(db, &swap).map_err(on_err),
    );

    match oracle {
        Some(e) if !e.symbols.is_empty() => {
            let e = Error::OraclesSkipped(e.symbols);
            warn!("{}", e);
        }
        _ => {}
    }
}

fn parse<'a>(
    st: &AppState,
    logs: impl Iterator<Item = &'a String> + 'a,
    sig: String,
) -> (
    Vec<db::RealizedPnl>,
    Vec<db::Liquidation>,
    Vec<db::Bankruptcy>,
    Vec<db::BalanceChange>,
    Vec<db::Swap>,
    Option<events::CacheOracleNoops>,
) {
    const PROG_LOG_PREFIX: &str = "Program log: ";

    let prog_start_str = format!("Program {} invoke", zo_abi::ID);
    let prog_end_str = format!("Program {} success", zo_abi::ID);

    let mut is_zo_log = false;

    let mut rpnl = Vec::new();
    let mut liq = Vec::new();
    let mut bank = Vec::new();
    let mut bal = Vec::new();
    let mut swap = Vec::new();
    let mut oracle = None;

    let time = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    for l in logs {
        if !is_zo_log {
            is_zo_log = l.starts_with(&prog_start_str);
            continue;
        }

        if l.starts_with(&prog_end_str) {
            is_zo_log = false;
            continue;
        }

        if !l.starts_with(PROG_LOG_PREFIX) {
            continue;
        }

        let l = &l[PROG_LOG_PREFIX.len()..];

        let bytes = match base64::decode(l) {
            Ok(x) => x,
            _ => continue,
        };

        if let Some(e) = load::<events::RealizedPnlLog>(&bytes) {
            if e.qty_paid == 0 {
                continue;
            }

            let symbol = st
                .iter_markets()
                .find(|x| x.dex_market == e.market_key)
                .unwrap()
                .symbol
                .into();

            rpnl.push(db::RealizedPnl {
                symbol,
                sig: sig.clone(),
                margin: e.margin.to_string(),
                is_long: e.is_long,
                pnl: e.pnl,
                qty_paid: e.qty_paid,
                qty_received: e.qty_received,
                time,
            });

            continue;
        }

        if let Some(e) = load::<events::LiquidationLog>(&bytes) {
            liq.push(db::Liquidation {
                sig: sig.clone(),
                liquidation_event: e.liquidation_event.to_string(),
                base_symbol: e.base_symbol.to_string(),
                quote_symbol: e.quote_symbol.unwrap_or_else(|| "".to_string()),
                liqor_margin: e.liqor_margin.to_string(),
                liqee_margin: e.liqee_margin.to_string(),
                assets_to_liqor: e.assets_to_liqor,
                quote_to_liqor: e.quote_to_liqor,
                time,
            });

            continue;
        }

        if let Some(e) = load::<events::BankruptcyLog>(&bytes) {
            bank.push(db::Bankruptcy {
                sig: sig.clone(),
                base_symbol: e.base_symbol.to_string(),
                liqor_margin: e.liqor_margin.to_string(),
                liqee_margin: e.liqee_margin.to_string(),
                assets_to_liqor: e.assets_to_liqor,
                quote_to_liqor: e.quote_to_liqor,
                insurance_loss: e.insurance_loss,
                socialized_loss: e.socialized_loss,
                time,
            });

            continue;
        }

        if let Some(e) = load::<events::DepositLog>(&bytes) {
            bal.push(db::BalanceChange {
                time,
                sig: sig.clone(),
                margin: e.margin_key.to_string(),
                symbol: st.zo_state.collaterals[e.col_index as usize]
                    .oracle_symbol
                    .into(),
                amount: e.deposit_amount as i64,
            });
        }

        if let Some(e) = load::<events::WithdrawLog>(&bytes) {
            bal.push(db::BalanceChange {
                time,
                sig: sig.clone(),
                margin: e.margin_key.to_string(),
                symbol: st.zo_state.collaterals[e.col_index as usize]
                    .oracle_symbol
                    .into(),
                amount: -(e.withdraw_amount as i64),
            })
        }

        if let Some(e) = load::<events::SwapLog>(&bytes) {
            swap.push(db::Swap {
                time,
                sig: sig.clone(),
                margin: e.margin_key.to_string(),
                base_symbol: st.zo_state.collaterals[e.base_index as usize]
                    .oracle_symbol
                    .into(),
                quote_symbol: st.zo_state.collaterals[e.quote_index as usize]
                    .oracle_symbol
                    .into(),
                base_delta: e.base_delta,
                quote_delta: e.quote_delta,
            });
        }

        if let Some(e) = load::<events::CacheOracleNoops>(&bytes) {
            oracle = Some(e);
        }
    }

    (rpnl, liq, bank, bal, swap, oracle)
}

#[inline(always)]
fn load<T: Event>(buf: &[u8]) -> Option<T> {
    match buf[..8] == T::discriminator() {
        true => T::deserialize(&mut &buf[8..]).ok(),
        false => None,
    }
}
