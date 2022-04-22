use mongodb::{
    bson::{doc, Document},
    error::{BulkWriteFailure, Error as MongoError, ErrorKind, WriteFailure},
    options::{IndexOptions, InsertManyOptions, ReplaceOptions, UpdateOptions},
    Collection, Database, IndexModel,
};
use serde::Serialize;
use std::{collections::HashMap, time::SystemTime};
use tracing::{debug, info};

#[derive(Serialize)]
pub struct Trade {
    symbol: String,
    time: i64,
    price: f64,
    side: String,
    size: f64,
    #[serde(rename = "isMaker")]
    is_maker: bool,
    control: String,
    #[serde(rename = "orderId")]
    order_id: String,
    #[serde(rename = "seqNum")]
    seq_num: i64,
}

#[derive(Serialize)]
pub struct Funding {
    pub symbol: String,
    #[serde(rename = "fundingIndex")]
    pub funding_index: String,
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
pub struct MarkTwap {
    #[serde(rename = "lastSampleStartTime")]
    pub last_sample_start_time: i64,
    pub symbol: String,
    pub twap: f64,
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
                    info!("inserted {} documents", xs.len() - es.len());
                    Ok(())
                }

                _ => Err(err),
            }
        }
        Ok(r) => {
            info!("inserted {} documents", r.inserted_ids.len());
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
}

impl Trade {
    #[tracing::instrument(
        skip_all,
        level = "error",
        name = "update_trade",
        fields(
            symbol = symbol,
            from = tracing::field::Empty,
            to = tracing::field::Empty,
        ),
    )]
    pub async fn update(
        db: &Database,
        symbol: &str,
        base_decimals: u8,
        quote_decimals: u8,
        buf: &[u8],
    ) -> Result<(), MongoError> {
        let time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let base_mul = 10f64.powi(base_decimals as i32);
        let quote_mul = 10f64.powi(quote_decimals as i32);

        let trades_coll = db.collection::<Self>("trades");
        let eq_coll = db.collection::<Document>("eventQueue");

        let last_seq_num = eq_coll
            .find_one(None, None)
            .await?
            .and_then(|doc| doc.get_i64(symbol).ok())
            .unwrap_or(0i64);

        let (trades, new_seq_num) =
            zo_abi::dex::Event::deserialize_since(buf, last_seq_num as u64)
                .unwrap();

        let new_seq_num = new_seq_num as i64;

        {
            let span = tracing::Span::current();
            span.record("last_seq_num", &last_seq_num);
            span.record("new_seq_num", &new_seq_num);
        }

        let trades: Vec<_> = trades
            .filter(|(_, e)| e.is_fill())
            .map(|(seq_num, e)| {
                let (side, price, size) = match e.is_bid() {
                    true => {
                        let price = match e.is_maker() {
                            true => e.native_qty_paid + e.native_fee_or_rebate,
                            false => e.native_qty_paid - e.native_fee_or_rebate,
                        };
                        let price = ((price as f64) * base_mul)
                            / ((e.native_qty_released as f64) * quote_mul);
                        let size = (e.native_qty_released as f64) / base_mul;

                        ("buy", price, size)
                    }
                    false => {
                        let price = match e.is_maker() {
                            true => {
                                e.native_qty_released - e.native_fee_or_rebate
                            }
                            false => {
                                e.native_qty_released + e.native_fee_or_rebate
                            }
                        };
                        let price = ((price as f64) * base_mul)
                            / ((e.native_qty_paid as f64) * quote_mul);
                        let size = (e.native_qty_paid as f64) / base_mul;

                        ("sell", price, size)
                    }
                };

                Self {
                    symbol: symbol.to_string(),
                    time,
                    price,
                    side: side.to_string(),
                    size,
                    is_maker: e.is_maker(),
                    control: e.control.to_string(),
                    order_id: format!("{:#x}", { e.order_id }),
                    seq_num: seq_num as i64,
                }
            })
            .collect();

        insert(
            &trades_coll,
            &trades,
            [
                IndexModel::builder()
                    .keys(doc! {
                        "symbol": 1, "control": 1, "orderId": 1, "seqNum": 1
                    })
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
                IndexModel::builder().keys(doc! { "time": 1 }).build(),
                IndexModel::builder().keys(doc! { "symbol": 1 }).build(),
            ],
        )
        .await?;

        // Do this after inserting documents to ensure that
        // the sequence number doesn't get updated with a
        // failed insertion.
        eq_coll
            .update_one(
                doc! {},
                doc! { "$set": { symbol: new_seq_num } },
                Some(UpdateOptions::builder().upsert(true).build()),
            )
            .await?;

        Ok(())
    }
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

impl MarkTwap {
    #[tracing::instrument(
        skip_all,
        level = "error",
        fields(coll = "markTwap", symbol = %x.symbol),
    )]
    pub async fn upsert(db: &Database, x: &Self) -> Result<(), MongoError> {
        let c = db.collection::<Self>("markTwap");

        c.create_indexes(
            [IndexModel::builder()
                .keys(doc! { "lastSampleStartTime": 1, "symbol": 1 })
                .options(IndexOptions::builder().unique(true).build())
                .build()],
            None,
        )
        .await?;

        let r = c
            .replace_one(
                doc! {
                    "lastSampleStartTime": x.last_sample_start_time,
                    "symbol": &x.symbol,
                },
                x,
                Some(ReplaceOptions::builder().upsert(true).build()),
            )
            .await;

        match r {
            Err(e) => match *e.kind {
                ErrorKind::Write(WriteFailure::WriteError(e))
                    if e.code == 11000 =>
                {
                    tracing::error!("testing");
                    debug!("ignored due to duplicate key");
                }
                _ => return Err(e),
            },
            Ok(r) => {
                debug!("updated {} documents", r.modified_count);
            }
        }

        Ok(())
    }
}
