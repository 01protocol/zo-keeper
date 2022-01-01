use crate::{error::Error, AppState};

pub async fn run(st: &'static AppState) -> Result<(), Error> {
    let handles = st.load_dex_markets().map(|(symbol, mkt)| {
        tokio::spawn(consumer_loop(st, symbol.clone(), mkt))
    });

    let _ = futures::future::join_all(handles);
    Ok(())
}

async fn consumer_loop(
    _st: &'static AppState,
    _symbol: String,
    _market: zo_abi::dex::ZoDexMarket,
) {
}
