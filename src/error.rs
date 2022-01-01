use tokio::sync::mpsc::Receiver;
use tracing::{error, Span};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Skipped oracles {}", .0.join(", "))]
    OraclesSkipped(Vec<String>),
    #[error("{0}")]
    AnchorClient(#[from] anchor_client::ClientError),
    #[error("{0}")]
    JsonRpc(#[from] jsonrpc_core_client::RpcError),
    #[error("{0}")]
    TransactionError(
        #[from] anchor_client::solana_sdk::transaction::TransactionError,
    ),
    #[error("{0}")]
    Db(#[from] mongodb::error::Error),
    #[error("{0}")]
    Var(#[from] std::env::VarError),
    #[error("Error decoding base64 log: {}", .0)]
    DecodingError(String),
}

pub type ErrorContext = (Span, Error);

pub async fn error_handler(mut rx: Receiver<ErrorContext>) {
    while let Some((span, e)) = rx.recv().await {
        // Filter out benign errors.
        match e {
            _ => {
                span.in_scope(|| {
                    error!("{}", e);
                    error!("{:?}", e);
                });
            }
        };
    }
}
