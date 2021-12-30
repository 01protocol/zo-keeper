use mongodb::{
    bson::{doc, Document},
    error::{BulkWriteFailure, Error as MongoError, ErrorKind},
    options::{IndexOptions, InsertManyOptions, UpdateOptions},
    Client as MongoClient, Collection, IndexModel,
};
use serde::Serialize;
use std::time::SystemTime;

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
    #[serde(rename = "lastUpdated")]
    pub last_updated: i64,
}

impl Trade {
    pub async fn update(
        client: &MongoClient,
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

        let db = client.database("main");
        let trades_coll = db.collection::<Self>("trades-tmp");
        let eq_coll = db.collection::<Document>("eventQueue-tmp");

        Self::execute_update(
            &trades_coll,
            &eq_coll,
            buf,
            time,
            symbol,
            base_mul,
            quote_mul,
        )
        .await?;

        Ok(())
    }

    async fn execute_update(
        trades_coll: &Collection<Self>,
        eq_coll: &Collection<Document>,
        buf: &[u8],
        time: i64,
        symbol: &str,
        base_mul: f64,
        quote_mul: f64,
    ) -> Result<(), MongoError> {
        // Indices are only created if they don't already exist.
        trades_coll
            .create_indexes(
                [
                    IndexModel::builder()
                        .keys(doc! {
                            "symbol": 1, "control": 1, "orderId": 1, "seqNum": 1
                        })
                        .options(IndexOptions::builder().unique(true).build())
                        .build(),
                    IndexModel::builder().keys(doc! { "time": 1 }).build(),
                    IndexModel::builder().keys(doc! { "symbol": 1 }).build(),
                ]
                .into_iter(),
                None,
            )
            .await?;

        let last_seq_num = eq_coll
            .find_one(None, None)
            .await?
            .and_then(|doc| doc.get_i64(symbol).ok())
            .unwrap_or(0i64);

        let (trades, new_seq_num) =
            zo_abi::dex::Event::deserialize_since(buf, last_seq_num as u64);

        let new_seq_num = new_seq_num as i64;

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
                    order_id: format!("0x{:x}", { e.order_id }),
                    seq_num: seq_num as i64,
                }
            })
            .collect();

        if trades.is_empty() {
            tracing::info!(
                "{}: no trades from {} to {} ",
                symbol,
                last_seq_num,
                new_seq_num
            );
        } else {
            let res = trades_coll
                .insert_many(
                    trades,
                    // > With unordered inserts, if an error occurs during an
                    // > insert of one of the documents, MongoDB continues to
                    // > insert the remaining documents in the array.
                    //
                    // https://docs.mongodb.com/v3.6/reference/method/db.collection.insert/#perform-an-unordered-insert
                    Some(InsertManyOptions::builder().ordered(false).build()),
                )
                .await;

            // We want to omit any duplicate key errors. The error code
            // for that is 11000, so if every error has code 11000 don't
            // raise an error.
            if let Err(ref error) = res {
                match *error.kind {
                    ErrorKind::BulkWrite(BulkWriteFailure {
                        write_errors: Some(ref es),
                        ..
                    }) if es.iter().all(|e| e.code == 11000) => {
                        tracing::info!(
                            "{}: events from {} to {} duplicate",
                            symbol,
                            last_seq_num,
                            new_seq_num
                        );
                    }

                    _ => {
                        res?;
                    }
                };
            }

            tracing::info!(
                "{}: inserted events from {} to {}",
                symbol,
                last_seq_num,
                new_seq_num
            );
        }

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

impl Funding {
    pub async fn update(
        c: &mongodb::Client,
        xs: &[Self],
    ) -> Result<(), MongoError> {
        if xs.is_empty() {
            return Ok(());
        }

        let coll = c.database("main").collection::<Self>("funding-tmp");

        coll.create_index(
            IndexModel::builder()
                .keys(doc! { "symbol": 1, "lastUpdated": 1 })
                .options(IndexOptions::builder().unique(true).build())
                .build(),
            None,
        )
        .await?;

        let res = coll.insert_many(xs, None).await;

        // Similar to trades collection, we want to omit any
        // duplicate key errors, which has error code 11000.
        if let Err(ref error) = res {
            match *error.kind {
                ErrorKind::BulkWrite(BulkWriteFailure {
                    write_errors: Some(ref es),
                    ..
                }) if es.iter().all(|e| e.code == 11000) => {}

                _ => {
                    res?;
                }
            };
        }

        Ok(())
    }
}
