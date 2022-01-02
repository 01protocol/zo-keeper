// NOTE: Modified implementation of anchor's parser because anchor's impl has a few issues

use crate::AppState;
use anchor_client::anchor_lang::{AnchorDeserialize, Event};
use tracing::error_span;

pub async fn parse<T: Event + AnchorDeserialize>(
    logs: impl Iterator<Item = String>,
    st: &AppState,
) -> Vec<T> {
    let span = error_span!("");

    let prog_start_str =
        format!("{} {} {}", "Program", st.program.id(), "invoke");
    let prog_end_str =
        format!("{} {} {}", "Program", st.program.id(), "success");

    let mut events: Vec<T> = Vec::new();
    let mut is_zo_log = false;

    for l in logs {
        let l = l.to_string();

        // check if zo program logs start, if not already started
        if !is_zo_log {
            is_zo_log = l.starts_with(&prog_start_str);
            continue;
        }

        // check if zo program ends, if already started
        if l.starts_with(&prog_end_str) {
            is_zo_log = false;
            continue;
        }

        // parse log for event
        if l.starts_with("Program log:") {
            let log = l.to_string().split_off("Program log: ".len());
            let borsh_bytes = match base64::decode(&log) {
                Ok(borsh_bytes) => borsh_bytes,
                _ => {
                    continue;
                }
            };

            let mut slice: &[u8] = &borsh_bytes[..];
            let disc: [u8; 8] = {
                let mut disc = [0; 8];
                disc.copy_from_slice(&borsh_bytes[..8]);
                slice = &slice[8..];
                disc
            };
            if disc == T::discriminator() {
                let e: Option<T> =
                    AnchorDeserialize::deserialize(&mut slice).ok();

                if let Some(e) = e {
                    events.push(e)
                } else {
                    st.error(
                        span.clone(),
                        crate::error::Error::DecodingError(log),
                    )
                    .await;
                }
            }
        };
    }

    events
}
