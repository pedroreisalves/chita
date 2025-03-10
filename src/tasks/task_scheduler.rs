use crate::core::{app, crystal};
use crate::helpers::config::{schedule_interval, start_time, stop_time};
use clokwerk::{Job, Scheduler};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

pub async fn start() {
    let scheduler = Arc::new(Mutex::new(Scheduler::with_tz(chrono::Utc)));
    let scheduler_clone = Arc::clone(&scheduler);

    {
        let mut scheduler = scheduler.lock().await;
        scheduler
            .every(schedule_interval())
            .at(&start_time())
            .run(|| {
                tokio::spawn(async {
                    app::run().await;
                });
            });
    }

    {
        let mut scheduler = scheduler.lock().await;
        scheduler
            .every(schedule_interval())
            .at(&stop_time())
            .run(|| {
                tokio::spawn(async {
                    crystal::stop().await;
                });
            });
    }

    tokio::spawn(async move {
        loop {
            {
                let mut scheduler = scheduler_clone.lock().await;
                scheduler.run_pending();
            }
            sleep(Duration::from_secs(5)).await;
        }
    });
}
