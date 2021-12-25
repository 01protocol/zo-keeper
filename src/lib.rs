pub mod keeper;
pub mod listener;

mod db;

use anchor_client::{
    solana_client::rpc_client::RpcClient, solana_sdk::pubkey::Pubkey, Client,
    Cluster, Program,
};

pub struct AppState {
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
