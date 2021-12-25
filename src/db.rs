use serde::Serialize;

#[derive(Serialize)]
pub struct Trades {
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
    seq_num: u32,
}

#[derive(Serialize)]
pub struct Funding {
    symbol: String,
    #[serde(rename = "fundingIndex")]
    funding_index: String,
    #[serde(rename = "lastUpdated")]
    last_updated: i64,
}
