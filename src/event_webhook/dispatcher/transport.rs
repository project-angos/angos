use std::time::Duration;

use reqwest::Client;

use super::signature::compute_signature;

pub(super) async fn send_request(
    client: &Client,
    url: &str,
    token: Option<&str>,
    body: &[u8],
    event_kind_header: &str,
) -> Result<(), String> {
    let mut request = client
        .post(url)
        .header("content-type", "application/json")
        .header("X-Registry-Event", event_kind_header);

    if let Some(token) = token {
        let signature = compute_signature(token, body);
        request = request
            .header("Authorization", format!("Bearer {token}"))
            .header("X-Registry-Signature-256", format!("sha256={signature}"));
    }

    let response = request
        .body(body.to_vec())
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !response.status().is_success() {
        return Err(format!("returned status {}", response.status()));
    }

    Ok(())
}

pub(super) async fn send_with_retries(
    client: &Client,
    url: &str,
    token: Option<&str>,
    body: &[u8],
    event_kind_header: &str,
    max_retries: u32,
) -> Result<(), String> {
    let mut last_err = None;

    for attempt in 0..=max_retries {
        if attempt > 0 {
            let backoff = Duration::from_millis(100 * 2u64.pow(attempt - 1));
            tokio::time::sleep(backoff).await;
        }

        match send_request(client, url, token, body, event_kind_header).await {
            Ok(()) => return Ok(()),
            Err(e) => last_err = Some(e),
        }
    }

    Err(last_err.unwrap_or_else(|| "unknown error".to_string()))
}
