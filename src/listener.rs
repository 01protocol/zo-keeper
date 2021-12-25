use crate::AppState;
use anchor_client::solana_client::pubsub_client::PubsubClient;

pub async fn run(st: &'static AppState) {
    futures::join!(
        listen_oracle_failures(st),
        listen_event_queue(st),
        listen_update_funding(st),
        poll_update_funding(st),
    );
}

async fn listen_oracle_failures(_st: &'static AppState) {}

async fn listen_event_queue(st: &'static AppState) {}

async fn listen_update_funding(_st: &'static AppState) {}

async fn poll_update_funding(_st: &'static AppState) {}
