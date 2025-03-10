use tokio::time::{sleep, Duration};

mod core;
mod helpers;
mod tasks;

use crate::tasks::task_scheduler;
use helpers::{
    config::{vault_url, KEEPALIVE, SENTRY_DSN},
    vault,
};

#[tokio::main]
async fn main() {
    let sentry_dsn = vault::get_secret(SENTRY_DSN, &vault_url()).await.unwrap();

    let _guard = sentry::init((
        sentry_dsn,
        sentry::ClientOptions {
            release: sentry::release_name!(),
            environment: Some("production".into()),
            ..Default::default()
        },
    ));

    tokio::spawn(async {
        task_scheduler::start().await;
    });

    loop {
        sleep(Duration::from_secs(KEEPALIVE)).await;
    }
}
