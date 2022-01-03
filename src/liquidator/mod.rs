use solana_sdk::pubkey::Pubkey;

use crate::{error::Error, AppState};

pub mod accounts;
pub mod error;
pub mod fetcher;
pub mod liquidation;
pub mod listener;
pub mod margin_utils;
pub mod math;
pub mod opts;
pub mod swap;
pub mod utils;

use opts::Opts;

pub async fn run(
    st: &'static AppState,
    dex_program: Pubkey,
    zo_program: Pubkey,
    serum_dex_program: Pubkey,
    num_workers: u8,
    n: u8,
) -> Result<(), Error> {
    tokio::task::spawn_blocking(move || {
        liquidation::start(
            &st,
            Opts {
                cluster: st.cluster.clone(),
                http_endpoint: st.cluster.url().to_string(),
                ws_endpoint: st.cluster.ws_url().to_string(),
                dex_program,
                zo_program,
                serum_dex_program,
                num_workers,
                n,
            },
        )
    })
    .await
    .unwrap();
    Ok(())
}
