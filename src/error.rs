use tokio::sync::mpsc::Receiver;
use tracing::{error, Span};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Skipped oracles {}", .0.join(", "))]
    OraclesSkipped(Vec<String>),

    // Library errors
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
}

pub type ErrorContext = (Span, Error);

pub async fn error_handler(mut rx: Receiver<ErrorContext>) {
    while let Some((span, e)) = rx.recv().await {
        use anchor_client::{
            solana_client::{
                client_error::{
                    ClientError as SolanaClientError,
                    ClientErrorKind as SolanaClientErrorKind,
                },
                rpc_request::{
                    RpcError as SolanaRpcError,
                    RpcResponseErrorData as SolanaRpcResponseErrorData,
                },
            },
            solana_sdk::transaction::TransactionError,
            ClientError as AnchorClientError,
        };
        use Error::*;

        // Filter out benign errors.
        match e {
            AnchorClient(AnchorClientError::SolanaClientError(
                SolanaClientError {
                    kind: SolanaClientErrorKind::RpcError(
                            SolanaRpcError::RpcResponseError {
                                data: SolanaRpcResponseErrorData::SendTransactionPreflightFailure(x),
                                ..
                            },
                        ),
                    ..
                },
            )) => if let Some(TransactionError::AlreadyProcessed) = x.err {}

            _ => {
                span.in_scope(|| {
                    error!("{}", e);
                    error!("{:?}", e);
                });
            }
        };
    }
}
