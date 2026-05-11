#[derive(Debug, PartialEq, thiserror::Error)]
pub enum Error {
    #[error("Policy evaluation error: {0}")]
    Evaluation(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evaluation_display_matches_original() {
        let err = Error::Evaluation("something went wrong".to_string());
        assert_eq!(
            err.to_string(),
            "Policy evaluation error: something went wrong"
        );
    }
}
