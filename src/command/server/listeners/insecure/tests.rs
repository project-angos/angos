use std::net::{IpAddr, Ipv4Addr};
use std::num::NonZeroU64;
use std::time::Duration;

use crate::command::server::listeners::insecure::InsecureListener;
use crate::command::server::server_context::tests::create_test_server_context;
use crate::configuration::listeners::ListenerBaseConfig;

#[test]
fn test_config_default_values() {
    let config = ListenerBaseConfig::default();

    assert_eq!(config.port, 8000);
    assert_eq!(config.query_timeout.get(), 3600);
    assert_eq!(config.query_timeout_grace_period.get(), 60);
    assert_eq!(config.bind_address, IpAddr::from(Ipv4Addr::from([0; 4])));
}

#[test]
fn test_config_rejects_zero_timeouts() {
    for key in ["query_timeout", "query_timeout_grace_period"] {
        let error = toml::from_str::<ListenerBaseConfig>(&format!("{key} = 0")).unwrap_err();

        assert!(error.to_string().contains(key), "got: {error}");
    }
}

// A reload must apply changed timeouts to the running listener, not only the
// server context.
#[tokio::test]
async fn test_hot_reload_updates_timeouts() {
    let listener_config = ListenerBaseConfig::default();
    let context = create_test_server_context().await;
    let listener = InsecureListener::new(&listener_config, context);
    assert_eq!(listener.timeouts.load().query, Duration::from_hours(1));

    let reloaded = ListenerBaseConfig {
        query_timeout: NonZeroU64::new(10).unwrap(),
        query_timeout_grace_period: NonZeroU64::new(5).unwrap(),
        ..ListenerBaseConfig::default()
    };
    listener.notify_config_change(&reloaded, create_test_server_context().await);

    assert_eq!(listener.timeouts.load().query, Duration::from_secs(10));
    assert_eq!(listener.timeouts.load().grace, Duration::from_secs(5));
}
