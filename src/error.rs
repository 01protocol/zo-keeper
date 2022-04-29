#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Skipped oracles {}", .0.join(", "))]
    OraclesSkipped(Vec<String>),
    #[error("Failed to confirm: {0}")]
    ConfirmationTimeout(anchor_client::solana_sdk::signature::Signature),

    // Library errors
    #[error("{0}: {0:?}")]
    AnchorClient(#[from] anchor_client::ClientError),
    #[error("{0}: {0:?}")]
    SolanaClient(#[from] solana_client::client_error::ClientError),
    #[error("{0}: {0:?}")]
    JsonRpc(#[from] jsonrpc_core_client::RpcError),
    #[error("{0}: {0:?}")]
    TransactionError(
        #[from] anchor_client::solana_sdk::transaction::TransactionError,
    ),
    #[error("{0}")]
    Db(#[from] mongodb::error::Error),
    #[error("{0}")]
    Var(#[from] std::env::VarError),
}
