use tokio::sync::mpsc;
use tracing::{Event, Level, Subscriber};
use tracing_bunyan_formatter::{JsonStorage, JsonStorageLayer};
use tracing_subscriber::{
    layer::{Context, Layer, SubscriberExt as _},
    registry::LookupSpan,
    util::SubscriberInitExt as _,
};

pub fn init(rx: mpsc::Sender<String>) {
    tracing_subscriber::fmt()
        .finish()
        .with(JsonStorageLayer)
        .with(DiscordWebhookLayer::new(rx))
        .init();
}

pub async fn notify_worker(
    mut rx: mpsc::Receiver<String>,
) -> Result<(), reqwest::Error> {
    let url_option: Option<String> = std::env::var("DISCORD_WEBHOOK_URL").ok();

    while let Some(s) = rx.recv().await {
        let url = match url_option.as_ref() {
            Some(x) => x,
            None => continue,
        };
        let _ = reqwest::Client::new()
            .post(url)
            .json(&serde_json::json!({ "content": s }))
            .send()
            .await
            .map_err(|e| eprintln!("{}", e));
    }

    Ok(())
}

pub struct DiscordWebhookLayer {
    tx: mpsc::Sender<String>,
}

impl DiscordWebhookLayer {
    fn new(tx: mpsc::Sender<String>) -> Self {
        Self { tx }
    }
}

impl<S> Layer<S> for DiscordWebhookLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, ev: &Event, cx: Context<S>) {
        let meta = ev.metadata();

        if *meta.level() > Level::WARN {
            return;
        }

        let time = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%:z");
        let mut m = String::new();

        m.push_str(&format!(
            "{} {} {}: {} ",
            time,
            meta.level(),
            meta.target(),
            meta.name()
        ));

        if let Some(scope) = cx.event_scope(ev) {
            let mut empty = true;

            for span in scope.from_root() {
                empty = false;

                let ext = span.extensions();
                let store = ext.get::<JsonStorage>().unwrap().values();

                if store.is_empty() {
                    continue;
                }

                m.push('{');
                m.push_str(
                    &store
                        .iter()
                        .map(|(k, v)| format!("{}={}", k, v))
                        .collect::<Vec<_>>()
                        .join(" "),
                );
                m.push('}');
            }

            if !empty {
                m.push_str(": ");
            }
        }

        let mut visitor = JsonStorage::default();
        ev.record(&mut visitor);

        let message = visitor
            .values()
            .get("message")
            .and_then(|v| match v {
                serde_json::Value::String(s) => Some(s.as_str()),
                _ => None,
            })
            .unwrap_or("");

        m.push_str(message);
        self.tx.try_send(m.to_string()).unwrap();
    }
}
