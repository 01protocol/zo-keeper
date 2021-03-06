mod accounts;
mod error;
mod liquidation;
mod listener;
mod margin_utils;
mod math;
mod swap;
mod utils;

use crate::{AppState, Error};

pub async fn run(
    st: &'static AppState,
    worker_count: u8,
    worker_index: u8,
) -> Result<(), Error> {
    let database = accounts::DbWrapper::new(st, worker_index, worker_count);

    let f = tokio::spawn(self::listener::start_listener(
        &zo_abi::ID,
        st.cluster.ws_url().to_string(),
        database.clone(),
    ));

    let g = tokio::spawn(self::liquidation::liquidate_loop(&st, database));

    // Propagate panic.
    tokio::select! {
        t = f => t.unwrap(),
        t = g => t.unwrap(),
    };

    Ok(())
}
