use crate::error::{Error, ErrorContext};
use anchor_client::{
    solana_client::rpc_client::RpcClient, solana_sdk::pubkey::Pubkey, Client,
    Cluster, Program,
};

pub struct AppState {
    pub err_tx: tokio::sync::mpsc::Sender<ErrorContext>,
    pub cluster: Cluster,
    pub client: Client,
    pub program: Program,
    pub rpc: RpcClient,
    pub zo_state: zo_abi::State,
    pub zo_cache: zo_abi::Cache,
    pub zo_state_pubkey: Pubkey,
    pub zo_cache_pubkey: Pubkey,
    pub zo_state_signer_pubkey: Pubkey,
}

impl AppState {
    pub async fn error<T: Into<Error>>(&self, s: tracing::Span, e: T) {
        self.err_tx.send((s, e.into())).await.unwrap()
    }

    pub fn iter_markets(
        &self,
    ) -> impl Iterator<Item = &zo_abi::PerpMarketInfo> {
        self.zo_state
            .perp_markets
            .iter()
            .filter(|market| market.dex_market != Pubkey::default())
    }

    pub fn load_dex_markets(
        &self,
    ) -> impl Iterator<Item = (String, zo_abi::dex::ZoDexMarket)> + '_ {
        self.iter_markets().map(|m| {
            (
                m.symbol.into(),
                zo_abi::dex::ZoDexMarket::deserialize(
                    &self.rpc.get_account_data(&m.dex_market).unwrap(),
                )
                .unwrap()
                .clone(),
            )
        })
    }

    pub fn iter_oracles(&self) -> impl Iterator<Item = &zo_abi::OracleCache> {
        self.zo_cache.oracles.iter().filter(|x| !x.symbol.is_nil())
    }
}
