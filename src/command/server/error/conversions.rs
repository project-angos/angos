use crate::{
    auth,
    command::{bootstrap, server::error::Error},
    configuration, event_webhook,
    jobs::store as job_store,
    metrics_provider, registry,
};

impl From<registry::Error> for Error {
    fn from(error: registry::Error) -> Self {
        match error {
            registry::Error::Initialization(msg) => Error::Initialization(msg),
            registry::Error::EventDelivery(msg) => Error::Execution(msg),
            error => {
                let (status_code, code, msg) = error.oci_answer();
                Error::Custom {
                    status_code,
                    code: code.to_string(),
                    msg,
                }
            }
        }
    }
}

impl From<auth::Error> for Error {
    fn from(e: auth::Error) -> Self {
        match e {
            auth::Error::Initialization(msg) => Error::Initialization(msg),
            auth::Error::Execution(msg) => Error::Execution(msg),
            auth::Error::Unauthorized(msg) => Error::Unauthorized(msg),
            auth::Error::ProviderUnavailable(msg) => Error::ProviderUnavailable(msg),
            // A registry error the authorizer surfaced keeps its OCI mapping.
            auth::Error::Registry(inner) => Error::from(*inner),
        }
    }
}

// Every bootstrap failure names what it was initializing in its own message.
impl From<bootstrap::Error> for Error {
    fn from(e: bootstrap::Error) -> Self {
        Error::Initialization(e.to_string())
    }
}

impl From<job_store::Error> for Error {
    fn from(e: job_store::Error) -> Self {
        Error::Initialization(e.to_string())
    }
}

impl From<metrics_provider::Error> for Error {
    fn from(error: metrics_provider::Error) -> Self {
        match error {
            metrics_provider::Error::Initialization(msg) => Error::Initialization(msg),
            metrics_provider::Error::Encode(msg) => Error::Internal(msg),
        }
    }
}

impl From<configuration::Error> for Error {
    fn from(error: configuration::Error) -> Self {
        match error {
            configuration::Error::Initialization(msg)
            | configuration::Error::InvalidFormat(msg)
            | configuration::Error::NotReadable(msg) => Error::Internal(msg),
        }
    }
}

impl From<event_webhook::Error> for Error {
    fn from(error: event_webhook::Error) -> Self {
        match error {
            event_webhook::Error::Initialization(msg) => Error::Initialization(msg),
            event_webhook::Error::Dispatch(msg) => Error::Execution(msg),
        }
    }
}
