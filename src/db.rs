use mongodb::{
    bson::doc,
    error::{BulkWriteFailure, Error as MongoError, ErrorKind},
    options::{IndexOptions, InsertManyOptions},
    Collection, Database, IndexModel,
};
use serde::Serialize;
use std::collections::HashMap;
use tracing::{debug, info};

#[derive(Serialize)]
pub struct Trade {
    pub symbol: String,
    pub time: i64,
    pub sig: String,
    pub price: f64,
    pub side: String,
    pub size: f64,
    #[serde(rename = "isMaker")]
    pub is_maker: bool,
    pub margin: String,
    pub control: String,
    pub discriminator: u16,
}

#[derive(Serialize)]
pub struct Funding {
    pub symbol: String,
    #[serde(rename = "fundingIndex")]
    pub funding_index: String,
    pub hourly: f64,
    #[serde(rename = "time")]
    pub time: i64,
}

#[derive(Serialize)]
pub struct RealizedPnl {
    pub symbol: String,
    pub sig: String,
    pub margin: String,
    #[serde(rename = "isLong")]
    pub is_long: bool,
    pub pnl: i64,
    #[serde(rename = "qtyPaid")]
    pub qty_paid: i64,
    #[serde(rename = "qtyReceived")]
    pub qty_received: i64,
    pub time: i64,
}

#[derive(Serialize)]
pub struct Liquidation {
    pub sig: String,
    #[serde(rename = "liquidationEvent")]
    pub liquidation_event: String,
    #[serde(rename = "baseSymbol")]
    pub base_symbol: String,
    #[serde(rename = "quoteSymbol")]
    pub quote_symbol: String,
    #[serde(rename = "liqorMargin")]
    pub liqor_margin: String,
    #[serde(rename = "liqeeMargin")]
    pub liqee_margin: String,
    #[serde(rename = "assetsToLiqor")]
    pub assets_to_liqor: i64,
    #[serde(rename = "quoteToLiqor")]
    pub quote_to_liqor: i64,
    pub time: i64,
}

#[derive(Serialize)]
pub struct Bankruptcy {
    pub sig: String,
    #[serde(rename = "baseSymbol")]
    pub base_symbol: String,
    #[serde(rename = "liqorMargin")]
    pub liqor_margin: String,
    #[serde(rename = "liqeeMargin")]
    pub liqee_margin: String,
    #[serde(rename = "assetsToLiqor")]
    pub assets_to_liqor: i64,
    #[serde(rename = "quoteToLiqor")]
    pub quote_to_liqor: i64,
    #[serde(rename = "insuranceLoss")]
    pub insurance_loss: i64,
    #[serde(rename = "socializedLoss")]
    pub socialized_loss: i64,
    pub time: i64,
}

#[derive(Serialize)]
pub struct BalanceChange {
    pub time: i64,
    pub sig: String,
    pub margin: String,
    pub symbol: String,
    pub amount: i64,
}

#[derive(Serialize)]
pub struct Swap {
    pub time: i64,
    pub sig: String,
    pub margin: String,
    #[serde(rename = "baseSymbol")]
    pub base_symbol: String,
    #[serde(rename = "quoteSymbol")]
    pub quote_symbol: String,
    #[serde(rename = "baseDelta")]
    pub base_delta: i64,
    #[serde(rename = "quoteDelta")]
    pub quote_delta: i64,
}

#[derive(Serialize)]
pub struct OpenInterest {
    time: i64,
    values: HashMap<String, i64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OtcFill {
    pub time: i64,
    pub sig: String,
    pub market: String,
    pub taker_margin: String,
    pub maker_margin: String,
    pub d_base: i64,
    pub d_quote: i64,
}

#[tracing::instrument(
    skip_all,
    level = "error",
    fields(coll = c.name()),
)]
async fn insert<T, const N: usize>(
    c: &Collection<T>,
    xs: &[T],
    indices: [IndexModel; N],
) -> Result<(), MongoError>
where
    T: Serialize,
{
    if xs.is_empty() {
        debug!("0 documents, skipping");
        return Ok(());
    }

    if !indices.is_empty() {
        c.create_indexes(indices, None).await?;
    }

    let res = c
        .insert_many(
            xs,
            // > With unordered inserts, if an error occurs during an
            // > insert of one of the documents, MongoDB continues to
            // > insert the remaining documents in the array.
            //
            // https://docs.mongodb.com/v3.6/reference/method/db.collection.insert/#perform-an-unordered-insert
            Some(InsertManyOptions::builder().ordered(false).build()),
        )
        .await;

    match res {
        Err(err) => {
            match *err.kind {
                // We want to skip any document that already exists. To
                // do so, we match explicitly against "duplicate key"
                // errors, which have the error code 11000. If every
                // error is a duplicate key error, then the error is
                // benign and canbe safely ignored.
                ErrorKind::BulkWrite(BulkWriteFailure {
                    write_errors: Some(ref es),
                    ..
                }) if es.iter().all(|e| e.code == 11000) => {
                    // Here, we know any failures that occured are
                    // because the document already exists in the DB.
                    // Thus, we can get the total number of documents
                    // inserted by subtracting out the "failed" inserts.
                    match xs.len() - es.len() {
                        0 => debug!("inserted 0 documents"),
                        l => info!("inserted {} documents", l),
                    }
                    Ok(())
                }

                _ => Err(err),
            }
        }
        Ok(r) => {
            match r.inserted_ids.len() {
                0 => debug!("inserted 0 documents"),
                l => info!("inserted {} documents", l),
            }
            Ok(())
        }
    }
}

macro_rules! simple_update_impl {
    { $( ($T:ty, $coll:expr, $idx:expr) ),* $(,)? } => {
        $(
            impl $T {
                pub async fn update(
                    db: &Database,
                    xs: &[$T],
                ) -> Result<(), MongoError> {
                    insert(
                        &db.collection::<$T>($coll),
                        xs,
                        [IndexModel::builder()
                            .keys($idx)
                            .options(IndexOptions::builder().unique(true).build())
                            .build()],
                    ).await
                }
            }
        )*
    }
}

simple_update_impl! {
    (Funding, "funding", doc! { "symbol": 1, "time": 1 }),
    (RealizedPnl, "rpnl", doc! {
        "sig": 1, "symbol": 1, "margin": 1, "pnl": 1
    }),
    (Liquidation, "liq", doc! {
        "sig": 1, "liqeeMargin": 1, "assetsToLiqor": 1
    }),
    (Bankruptcy, "bank", doc! {
        "sig": 1, "liqeeMargin": 1, "assetsToLiqor": 1
    }),
    (BalanceChange, "balanceChange", doc! {
        "sig": 1, "symbol": 1, "margin": 1, "amount": 1,
    }),
    (Swap, "swap", doc! {
        "sig": 1,
        "baseSymbol": 1, "quoteSymbol": 1,
        "baseDelta": 1, "quoteDelta": 1,
    }),
    (OtcFill, "otc", doc! {
        "sig": 1, "market": 1, "takerMargin": 1,
        "dBase": 1, "dQuote": 1,
    }),
    (Trade, "trades", doc! {
        "sig": 1, "discriminator": 1,
        "symbol": 1, "price": 1, "side": 1, "size": 1,
        "isMaker": 1, "control": 1,
    }),
}

impl OpenInterest {
    pub async fn insert(
        db: &Database,
        time: i64,
        values: HashMap<String, i64>,
    ) -> Result<(), MongoError> {
        insert(
            &db.collection::<Self>("oi"),
            &[Self { time, values }],
            [IndexModel::builder().keys(doc! { "time": 1 }).build()],
        )
        .await
    }
}
