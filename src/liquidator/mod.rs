mod accounts;
mod error;
mod fetcher;
mod liquidation;
mod listener;
mod margin_utils;
mod math;
mod opts;
mod swap;
mod utils;

use crate::{AppState, Error};

pub async fn run(
    st: &'static AppState,
    num_workers: u8,
    n: u8,
) -> Result<(), Error> {
    let options = self::opts::Opts {
        cluster: st.cluster.clone(),
        http_endpoint: st.cluster.url().to_string(),
        ws_endpoint: st.cluster.ws_url().to_string(),
        dex_program: zo_abi::dex::ID,
        zo_program: zo_abi::ID,
        serum_dex_program: zo_abi::serum::ID,
        num_workers,
        n,
    };
    let database = accounts::DbWrapper::new(&options, &st.program.payer());

    let options: &'static _ = Box::leak(Box::new(options));

    let f = tokio::spawn(self::listener::start_listener(
        &zo_abi::ID,
        st.cluster.ws_url().to_string(),
        database.clone(),
        num_workers,
        n,
    ));

    let g = tokio::spawn(self::liquidation::liquidate_loop(
        &st.client,
        database,
        options.clone(),
        st.program.payer(),
    ));

    // Propagate panic.
    tokio::select! {
        t = f => t.unwrap(),
        t = g => t.unwrap(),
    };

    Ok(())
}
