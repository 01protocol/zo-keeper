use anchor_client::{
    solana_client::{
        rpc_client::GetConfirmedSignaturesForAddress2Config,
        rpc_config::RpcTransactionConfig,
        rpc_response::RpcConfirmedTransactionStatusWithSignature,
    },
    solana_sdk::{commitment_config::CommitmentConfig, signature::Signature},
};
use solana_transaction_status::UiTransactionEncoding;
use std::str::FromStr;
use time::format_description::well_known::Rfc3339;

pub async fn run(
    st: &'static crate::AppState,
    mut before: Signature,
    until: i64,
) -> Result<(), crate::Error> {
    let rt = tokio::runtime::Handle::try_current().unwrap();

    let db = mongodb::Client::with_uri_str(std::env::var("DATABASE_URL")?)
        .await?
        .database(crate::recorder::DB_NAME);

    let mut last_time = time::OffsetDateTime::now_utc().unix_timestamp();

    while last_time >= until {
        tracing::info!(
            "{}: {}",
            time::OffsetDateTime::from_unix_timestamp(last_time)
                .unwrap()
                .format(&Rfc3339)
                .unwrap(),
            before,
        );

        let before_ = before.clone();
        let txs = rt
            .spawn_blocking(move || {
                st.rpc.get_signatures_for_address_with_config(
                    &zo_abi::ID,
                    GetConfirmedSignaturesForAddress2Config {
                        before: Some(before_),
                        until: None,
                        limit: Some(200),
                        commitment: Some(CommitmentConfig::finalized()),
                    },
                )
            })
            .await
            .unwrap()?;

        if txs.is_empty() {
            tracing::warn!("no transaction found before {}, quitting", before);
            return Ok(());
        }

        before = Signature::from_str(&txs[txs.len() - 1].signature).unwrap();
        last_time = txs[txs.len() - 1].block_time.unwrap();

        futures::future::join_all(
            txs.into_iter()
                .map(|tx| tokio::spawn(process_tx(st, db.clone(), tx))),
        )
        .await;
    }

    Ok(())
}

async fn process_tx(
    st: &'static crate::AppState,
    db: mongodb::Database,
    tx: RpcConfirmedTransactionStatusWithSignature,
) {
    if tx.err.is_some() {
        return;
    }

    let sg = Signature::from_str(&tx.signature).unwrap();

    let r = tokio::task::spawn_blocking(move || {
        st.rpc.get_transaction_with_config(
            &sg,
            RpcTransactionConfig {
                encoding: Some(UiTransactionEncoding::Base64),
                commitment: Some(CommitmentConfig::finalized()),
            },
        )
    })
    .await
    .unwrap();

    let r = match r {
        Ok(x) => x,
        Err(e) => {
            tracing::warn!(
                "failed to retrieve {}, skipping: {}",
                sg,
                crate::Error::from(e)
            );
            return;
        }
    };

    let time = match r.block_time {
        Some(t) => t,
        None => {
            tracing::warn!("missing block_time on {}, skipping", sg);
            return;
        }
    };

    if let Some(ss) = r.transaction.meta.and_then(|x| x.log_messages) {
        crate::events::process(st, &db, ss, tx.signature, time).await;
    }
}
