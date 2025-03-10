use clokwerk::Interval::*;
use clokwerk::{Interval, TimeUnits};
use std::env;

pub const RETRY_COUNT: usize = 30;
pub const TIMEOUT_DURATION: tokio::time::Duration = tokio::time::Duration::from_secs(5);
pub const UPLOAD_TIMEOUT_DURATION: tokio::time::Duration = tokio::time::Duration::from_secs(20);
pub const RETRY_DELAY: tokio::time::Duration = tokio::time::Duration::from_secs(1);
pub const BLOB_ACCOUNT: &str = "blob-account";
pub const BLOB_CONTAINER: &str = "blob-container";
pub const BLOB_KEY: &str = "blob-key";
pub const MARKETDATA_ADDRESS: &str = "marketdata-address";
pub const MARKETDATA_UN: &str = "marketdata-username";
pub const MARKETDATA_PW: &str = "marketdata-password";
pub const SENTRY_DSN: &str = "sentry-dsn";
pub const KEEPALIVE: u64 = 3600; // 1h

// * Format: Weekdays | Everyday
pub fn schedule_interval() -> Interval {
    if env::var("INTERVAL").unwrap_or_else(|_| "".to_string()) == "Everyday" {
        1.day()
    } else if env::var("INTERVAL").unwrap_or_else(|_| "".to_string()) == "Weekdays" {
        Weekday
    } else {
        Weekday
    }
}

// * Format: https:/${prod-fms-kv}.vault.azure.net/
pub fn vault_url() -> String {
    env::var("VAULT_URL").unwrap_or_else(|_| "".to_string())
}

// * Format: 11:00 (UTC+0)
pub fn start_time() -> String {
    env::var("CHITA_START_TIME").unwrap_or_else(|_| "11:00".to_string())
}

// * Format: 22:00 (UTC+0)
pub fn stop_time() -> String {
    env::var("CHITA_STOP_TIME").unwrap_or_else(|_| "22:00".to_string())
}
