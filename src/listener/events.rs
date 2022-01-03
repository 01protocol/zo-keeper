// NOTE: Modified implementation of anchor's parser because anchor's impl has a few issues

use crate::{db, error::Error, AppState};
use anchor_client::anchor_lang::Event;
use futures::TryFutureExt;
use tracing::error_span;
use zo_abi::events;

pub async fn process(
    st: &AppState,
    db: &mongodb::Database,
    ss: &[String],
    sig: String,
    slot: i64,
) {
    let span = error_span!("process");
    let (rpnl, liq, bank, oracle) = parse(st, ss.iter(), sig, slot);

    let on_err = |e| async { st.error(span.clone(), e).await };

    let _ = futures::join!(
        db::RealizedPnl::update(db, &rpnl).map_err(on_err),
        db::Liquidation::update(db, &liq).map_err(on_err),
        db::Bankruptcy::update(db, &bank).map_err(on_err),
    );

    match oracle {
        Some(e) if !e.symbols.is_empty() => {
            st.error(span.clone(), Error::OraclesSkipped(e.symbols))
                .await;
        }
        _ => {}
    }
}

fn parse<'a>(
    st: &AppState,
    logs: impl Iterator<Item = &'a String> + 'a,
    sig: String,
    slot: i64,
) -> (
    Vec<db::RealizedPnl>,
    Vec<db::Liquidation>,
    Vec<db::Bankruptcy>,
    Option<events::CacheOracleNoops>,
) {
    const PROG_LOG_PREFIX: &str = "Program log: ";

    let prog_start_str = format!("Program {} invoke", st.program.id());
    let prog_end_str = format!("Program {} success", st.program.id());

    let mut is_zo_log = false;

    let mut rpnl = Vec::new();
    let mut liq = Vec::new();
    let mut bank = Vec::new();
    let mut oracle = None;

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
                slot,
                margin: e.margin.to_string(),
                is_long: e.is_long,
                pnl: e.pnl,
                qty_paid: e.qty_paid,
                qty_received: e.qty_received,
            });

            continue;
        }

        if let Some(e) = load::<events::LiquidationLog>(&bytes) {
            liq.push(db::Liquidation {
                sig: sig.clone(),
                slot,
                liquidation_event: e.liquidation_event.to_string(),
                base_symbol: e.base_symbol.to_string(),
                quote_symbol: e.quote_symbol.unwrap_or_else(|| "".to_string()),
                liqor_margin: e.liqor_margin.to_string(),
                liqee_margin: e.liqee_margin.to_string(),
                assets_to_liqor: e.assets_to_liqor,
                quote_to_liqor: e.quote_to_liqor,
            });

            continue;
        }

        if let Some(e) = load::<events::BankruptcyLog>(&bytes) {
            bank.push(db::Bankruptcy {
                sig: sig.clone(),
                slot,
                base_symbol: e.base_symbol.to_string(),
                liqor_margin: e.liqor_margin.to_string(),
                liqee_margin: e.liqee_margin.to_string(),
                assets_to_liqor: e.assets_to_liqor,
                quote_to_liqor: e.quote_to_liqor,
                insurance_loss: e.insurance_loss,
                socialized_loss: e.socialized_loss,
            });

            continue;
        }

        if let Some(e) = load::<events::CacheOracleNoops>(&bytes) {
            oracle = Some(e);
        }
    }

    (rpnl, liq, bank, oracle)
}

#[inline(always)]
fn load<T: Event>(mut buf: &[u8]) -> Option<T> {
    match buf[..8] == T::discriminator() {
        true => T::deserialize(&mut buf).ok(),
        false => None,
    }
}
