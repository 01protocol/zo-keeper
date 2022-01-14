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

pub fn run(st: &'static AppState, num_workers: u8, n: u8) -> Result<(), Error> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
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

    self::listener::start_listener(
        &rt,
        &options.zo_program,
        &options.ws_endpoint,
        database.get_clone(),
        &options.num_workers,
        &options.n,
    );

    self::liquidation::liquidate_loop(
        rt,
        &st.client,
        database,
        options,
        st.program.payer(),
    );

    Ok(())
}
