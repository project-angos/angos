use redis::RedisError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("cache execution error: {0}")]
    Execution(String),
    #[error("redis error: {0}")]
    Redis(#[from] RedisError),
}
