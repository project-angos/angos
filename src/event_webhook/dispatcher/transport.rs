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
    let mut first_err: Option<String> = None;
    let mut last_err: Option<String> = None;
    let mut attempts: u32 = 0;

    for attempt in 0..=max_retries {
        if attempt > 0 {
            tokio::time::sleep(backoff_for_attempt(attempt)).await;
        }

        attempts += 1;
        match send_request(req).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(e.clone());
                }
                last_err = Some(e);
            }
        }
    }

    Err(format_retry_failure(
        attempts,
        first_err.as_deref(),
        last_err.as_deref(),
    ))
}

fn format_retry_failure(attempts: u32, first: Option<&str>, last: Option<&str>) -> String {
    match (first, last) {
        (None, _) | (_, None) => format!("after {attempts} attempt(s): unknown error"),
        (Some(f), Some(l)) if f == l => {
            format!("after {attempts} attempt(s): {f}")
        }
        (Some(f), Some(l)) => {
            format!("after {attempts} attempt(s); first error: {f}; last error: {l}")
        }
    }
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

    #[test]
    fn format_retry_failure_one_attempt_identical_error() {
        assert_eq!(
            format_retry_failure(1, Some("connection refused"), Some("connection refused")),
            "after 1 attempt(s): connection refused"
        );
    }

    #[test]
    fn format_retry_failure_three_attempts_identical_error() {
        assert_eq!(
            format_retry_failure(3, Some("connection refused"), Some("connection refused")),
            "after 3 attempt(s): connection refused"
        );
    }

    #[test]
    fn format_retry_failure_three_attempts_different_errors() {
        assert_eq!(
            format_retry_failure(3, Some("timeout"), Some("503 Service Unavailable")),
            "after 3 attempt(s); first error: timeout; last error: 503 Service Unavailable"
        );
    }

    #[test]
    fn format_retry_failure_none_is_defensive_unknown() {
        assert_eq!(
            format_retry_failure(0, None, None),
            "after 0 attempt(s): unknown error"
        );
    }
}
