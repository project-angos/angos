use std::time::Duration;

use reqwest::Client;

use super::signature::compute_signature;

pub struct DeliveryRequest<'a> {
    pub client: &'a Client,
    pub url: &'a str,
    pub token: Option<&'a str>,
    pub body: &'a [u8],
    pub event_kind_header: &'a str,
}

pub async fn send_request(req: &DeliveryRequest<'_>) -> Result<(), String> {
    let mut request = req
        .client
        .post(req.url)
        .header("content-type", "application/json")
        .header("X-Registry-Event", req.event_kind_header);

    if let Some(token) = req.token {
        let signature = compute_signature(token, req.body);
        request = request
            .header("Authorization", format!("Bearer {token}"))
            .header("X-Registry-Signature-256", format!("sha256={signature}"));
    }

    let response = request
        .body(req.body.to_vec())
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !response.status().is_success() {
        return Err(format!("returned status {}", response.status()));
    }

    Ok(())
}

pub async fn send_with_retries(req: &DeliveryRequest<'_>, max_retries: u32) -> Result<(), String> {
    let mut last_err = None;

    for attempt in 0..=max_retries {
        if attempt > 0 {
            tokio::time::sleep(backoff_for_attempt(attempt)).await;
        }

        match send_request(req).await {
            Ok(()) => return Ok(()),
            Err(e) => last_err = Some(e),
        }
    }

    Err(last_err.unwrap_or_else(|| "unknown error".to_string()))
}

// `attempt` must be >= 1. Both operations saturate at u64::MAX rather than
// panicking, so an unexpectedly large value yields Duration::from_millis(u64::MAX)
// (~584 million years) instead of an arithmetic panic.
fn backoff_for_attempt(attempt: u32) -> Duration {
    Duration::from_millis(100u64.saturating_mul(2u64.saturating_pow(attempt - 1)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_exponentially() {
        assert_eq!(backoff_for_attempt(1), Duration::from_millis(100));
        assert_eq!(backoff_for_attempt(2), Duration::from_millis(200));
        assert_eq!(backoff_for_attempt(3), Duration::from_millis(400));
    }

    #[test]
    fn backoff_saturates_for_huge_attempt() {
        assert_eq!(backoff_for_attempt(100), Duration::from_millis(u64::MAX));
    }
}
