use anyhow::{anyhow, Result};
use reqwest::Client;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::warn;
use urlencoding::encode;

#[derive(Debug, Deserialize)]
struct MatchResponse {
    message: MatchMessage,
}

#[derive(Debug, Deserialize)]
struct MatchMessage {
    items: Vec<MatchItem>,
}

#[derive(Debug, Deserialize)]
struct MatchItem {
    id: String,
    confidence: f64,
}

pub struct RorClient {
    client: Client,
    base_url: String,
    semaphore: Arc<Semaphore>,
}

impl RorClient {
    pub fn new(base_url: String, concurrency: usize, timeout_secs: u64) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .build()
            .expect("Failed to build HTTP client");

        Self {
            client,
            base_url,
            semaphore: Arc::new(Semaphore::new(concurrency)),
        }
    }

    /// Returns `Ok(Some((ror_id, confidence)))` on match, `Ok(None)` on no match,
    /// `Err` on transport/5xx failure after retries.
    pub async fn query_funder(&self, name: &str, task: &str) -> Result<Option<(String, f64)>> {
        let _permit = self.semaphore.acquire().await?;

        let url = format!(
            "{}/match?task={}&input={}",
            self.base_url,
            encode(task),
            encode(name)
        );

        let max_retries = 3;
        for attempt in 0..max_retries {
            match self.client.get(&url).send().await {
                Ok(response) => {
                    let status = response.status();
                    if status.is_success() {
                        let parsed: MatchResponse = response.json().await?;
                        return Ok(parsed.message.items.into_iter().next().map(|i| (i.id, i.confidence)));
                    } else if status.as_u16() == 429 {
                        let wait = response
                            .headers()
                            .get("Retry-After")
                            .and_then(|v| v.to_str().ok())
                            .and_then(|v| v.parse::<u64>().ok())
                            .unwrap_or(2u64.pow(attempt as u32));
                        warn!("Rate limited, waiting {}s", wait);
                        tokio::time::sleep(Duration::from_secs(wait)).await;
                        continue;
                    } else {
                        return Err(anyhow!("HTTP {}", status));
                    }
                }
                Err(e) => {
                    if attempt < max_retries - 1 {
                        let wait = 2u64.pow(attempt as u32);
                        warn!("Request error, retrying in {}s: {}", wait, e);
                        tokio::time::sleep(Duration::from_secs(wait)).await;
                        continue;
                    }
                    return Err(e.into());
                }
            }
        }

        Err(anyhow!("Max retries exceeded"))
    }
}
