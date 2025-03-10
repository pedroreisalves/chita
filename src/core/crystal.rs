use chrono::{DateTime, Local};
use sentry::Level;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::BufWriter;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{interval, sleep, Duration};

use super::crystal_params::CrystalParams;
use super::futures;
use crate::helpers::config::{vault_url, BLOB_ACCOUNT, BLOB_CONTAINER, BLOB_KEY};
use crate::helpers::storage;
use crate::helpers::vault;

static STOP_FLAG: AtomicBool = AtomicBool::new(false);

const MAX_BUFFER_SIZE: usize = 1000000;
const BATCH_SIZE: usize = 10000;
const NUM_WRITERS: usize = 20;
const FLUSH_INTERVAL: u64 = 300;
const RETRY_INTERVAL: u64 = 5;
const MAX_RETRIES: usize = 10;
const RECONNECT_DELAY: u64 = 10;

pub fn start(
    assets: Vec<String>,
    mkt_data_address: String,
    mkt_data_username: String,
    mkt_data_password: String,
) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send>> {
    Box::pin(start_async(CrystalParams {
        assets,
        mkt_data_address,
        mkt_data_username,
        mkt_data_password,
    }))
}

async fn start_async(params: CrystalParams) -> Result<(), Box<dyn std::error::Error>> {
    STOP_FLAG.store(false, Ordering::SeqCst);

    let params = Arc::new(params);

    let server_host = params.mkt_data_address.clone();
    let stream = match TcpStream::connect(server_host.clone()).await {
        Ok(s) => s,
        Err(e) => {
            let error_message = format!("Error: connect to Crystal - {:?}", e);
            sentry::capture_error(&Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                error_message.clone(),
            )));
            eprintln!("{}", error_message);
            reconnect(params.clone()).await?;
            return Ok(());
        }
    };
    let stream = Arc::new(Mutex::new(stream));

    let mut writers: Vec<Arc<Mutex<BufWriter<tokio::fs::File>>>> = Vec::new();
    let mut txs: Vec<mpsc::Sender<Vec<(DateTime<Local>, Vec<u8>)>>> = Vec::new();

    let message_count = Arc::new(AtomicUsize::new(0));
    let total_lines_sent = Arc::new(AtomicUsize::new(0));
    let message_count_report = Arc::clone(&message_count);
    let total_lines_sent_report = Arc::clone(&total_lines_sent);

    for i in 0..NUM_WRITERS {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(format!("content/crystal-md-{}.txt", i))
            .await?;
        let writer = Arc::new(Mutex::new(BufWriter::new(file)));
        let (tx, mut rx) = mpsc::channel::<Vec<(DateTime<Local>, Vec<u8>)>>(MAX_BUFFER_SIZE);
        txs.push(tx);

        let writer_clone = Arc::clone(&writer);
        let message_count_clone = Arc::clone(&message_count);

        tokio::spawn(async move {
            let mut flush_interval = interval(Duration::from_secs(FLUSH_INTERVAL));
            loop {
                if STOP_FLAG.load(Ordering::SeqCst) {
                    println!("Stopping writer {}", i);
                    break;
                }
                tokio::select! {
                    Some(batch) = rx.recv() => {
                        let mut writer = writer_clone.lock().await;
                        for (timestamp, line) in &batch {
                            let timestamp_str = timestamp.format("%H:%M:%S%.3f").to_string();
                            let line_with_timestamp =
                                format!("{} {}", timestamp_str, String::from_utf8_lossy(&line));
                            let mut retries = 0;
                            loop {
                                if let Err(e) = writer.write_all(line_with_timestamp.as_bytes()).await {
                                    println!("Error: write to file {} - {:?} at {:?}, retrying [{}/{}]", i, e, timestamp, retries + 1, MAX_RETRIES);
                                    retries += 1;
                                    if retries >= MAX_RETRIES {
                                        println!("Error: max retries reached for writer {} - {:?} at {:?}", i, e, timestamp);
                                        return;
                                    }
                                    sleep(Duration::from_secs(RETRY_INTERVAL)).await;
                                } else {
                                    break;
                                }
                            }
                        }
                        let mut retries = 0;
                        loop {
                            if let Err(e) = writer.flush().await {
                                println!("Error: flush writer {} - {:?} at periodic flush, retrying [{}/{}]", i, e, retries + 1, MAX_RETRIES);
                                retries += 1;
                                if retries >= MAX_RETRIES {
                                    println!("Error: max retries reached for flush on writer {} - {:?}", i, e);
                                    return;
                                }
                                sleep(Duration::from_secs(RETRY_INTERVAL)).await;
                            } else {
                                break;
                            }
                        }
                        message_count_clone.fetch_sub(batch.len(), Ordering::SeqCst);
                    },
                    _ = flush_interval.tick() => {
                        let mut writer = writer_clone.lock().await;
                        let mut retries = 0;
                        loop {
                            if let Err(e) = writer.flush().await {
                                println!("Error: flush writer {} - {:?}, retrying [{}/{}]", i, e, retries + 1, MAX_RETRIES);
                                retries += 1;
                                if retries >= MAX_RETRIES {
                                    println!("Error: max retries reached for flush on writer {} - {:?}", i, e);
                                    return;
                                }
                                sleep(Duration::from_secs(RETRY_INTERVAL)).await;
                            } else {
                                break;
                            }
                        }
                    }
                }
            }
        });

        writers.push(writer);
    }

    let mut read_buffer = Vec::with_capacity(16384);

    let futures = futures::get_futures();

    let mut subscriptions: Vec<String> = futures
        .into_iter()
        .chain(params.assets.clone().into_iter())
        .collect();

    let connected = Arc::new(Mutex::new(false));
    let connected_clone = Arc::clone(&connected);
    let stream_clone = Arc::clone(&stream);

    tokio::spawn({
        let params = params.clone();
        async move {
            let connected = connected_clone;
            while !*connected.lock().await {
                sleep(Duration::from_millis(100)).await;
            }

            while !subscriptions.is_empty() {
                let chunk_size = std::cmp::min(5000, subscriptions.len());
                let chunk: Vec<_> = subscriptions.split_off(subscriptions.len() - chunk_size);
                for item in chunk {
                    let mut stream = stream_clone.lock().await;
                    let command = format!("BQT {}\n", item.to_lowercase());
                    if let Err(e) = stream.write_all(command.as_bytes()).await {
                        println!("Error: BQT command - {:?}", e);
                        reconnect(params.clone()).await.ok();
                        return;
                    }
                    let command = format!("GQT {} S 1\n", item.to_lowercase());
                    if let Err(e) = stream.write_all(command.as_bytes()).await {
                        println!("Error: GQT command - {:?}", e);
                        reconnect(params.clone()).await.ok();
                        return;
                    }
                    let command = format!("SQT {}\n", item.to_lowercase());
                    if let Err(e) = stream.write_all(command.as_bytes()).await {
                        println!("Error: SQT command - {:?}", e);
                        reconnect(params.clone()).await.ok();
                        return;
                    }
                }
                sleep(Duration::from_secs(5)).await;
            }
        }
    });

    tokio::spawn(async move {
        loop {
            if STOP_FLAG.load(Ordering::SeqCst) {
                println!("Stopping cb");
                break;
            }
            sleep(Duration::from_secs(1)).await;
            println!(
                "cb: {}, cr: {}",
                message_count_report.load(Ordering::SeqCst),
                total_lines_sent_report.load(Ordering::SeqCst)
            );
        }
    });

    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut writer_index = 0;

    loop {
        if STOP_FLAG.load(Ordering::SeqCst) {
            println!("Stopping main loop");
            break;
        }

        let mut chunk = vec![0; 16384];
        let nbytes = {
            let mut stream = stream.lock().await;
            match stream.read(&mut chunk).await {
                Ok(n) => n,
                Err(e) => {
                    println!("Error: read stream - {:?}", e);
                    reconnect(params.clone()).await?;
                    return Ok(());
                }
            }
        };
        if nbytes == 0 {
            println!("FIN");
            reconnect(params.clone()).await?;
            return Ok(());
        }

        read_buffer.extend_from_slice(&chunk[..nbytes]);

        let mut pos = 0;
        while let Some(newline_pos) = read_buffer[pos..].iter().position(|&x| x == b'\n') {
            let line_end = pos + newline_pos + 1;
            let line = &read_buffer[pos..line_end];
            pos = line_end;

            let now: DateTime<Local> = Local::now();
            batch.push((now, line.to_vec()));

            if batch.len() >= BATCH_SIZE {
                if let Err(_) = txs[writer_index].send(batch.clone()).await {
                    println!("[DROP]");
                    break;
                }
                message_count.fetch_add(batch.len(), Ordering::SeqCst);
                total_lines_sent.fetch_add(batch.len(), Ordering::SeqCst);
                batch.clear();
                writer_index = (writer_index + 1) % NUM_WRITERS;
            }

            if let Ok(line_string) = std::str::from_utf8(line) {
                if line_string.contains("Connecting...") {
                    let mut stream = stream.lock().await;
                    if let Err(e) = stream.write_all(b"\n").await {
                        println!("Error: send ln - {:?}", e);
                        reconnect(params.clone()).await?;
                        return Ok(());
                    }
                } else if line_string.contains("Username:") {
                    let mut stream = stream.lock().await;
                    if let Err(e) = stream.write_all(params.mkt_data_username.as_bytes()).await {
                        println!("Error: send username - {:?}", e);
                        reconnect(params.clone()).await?;
                        return Ok(());
                    }
                } else if line_string.contains("Password:") {
                    let mut stream = stream.lock().await;
                    if let Err(e) = stream.write_all(params.mkt_data_password.as_bytes()).await {
                        println!("Error: send password - {:?}", e);
                        reconnect(params.clone()).await?;
                        return Ok(());
                    }
                } else if line_string.contains("You are connected") {
                    let mut connected = connected.lock().await;
                    *connected = true;
                }
            }
        }

        if pos == read_buffer.len() {
            read_buffer.clear();
        } else {
            read_buffer.drain(..pos);
        }
    }

    if !batch.is_empty() {
        println!(
            "Sending f-batch to writer ({} messages) - W:{}",
            batch.len(),
            writer_index
        );
        if let Err(_) = txs[writer_index].send(batch.clone()).await {
            println!("[f-DROP]");
        }
        message_count.fetch_add(batch.len(), Ordering::SeqCst);
        total_lines_sent.fetch_add(batch.len(), Ordering::SeqCst);
    }

    Ok(())
}

async fn reconnect(params: Arc<CrystalParams>) -> Result<(), Box<dyn std::error::Error>> {
    STOP_FLAG.store(true, Ordering::SeqCst);
    sleep(Duration::from_secs(RECONNECT_DELAY)).await;
    STOP_FLAG.store(false, Ordering::SeqCst);
    println!("Reconnecting...");
    sentry::capture_message("CMDC - RCT", Level::Info);
    start(
        params.assets.clone(),
        params.mkt_data_address.clone(),
        params.mkt_data_username.clone(),
        params.mkt_data_password.clone(),
    )
    .await?;
    Ok(())
}

pub async fn stop() {
    STOP_FLAG.store(true, Ordering::SeqCst);
    println!("[Stop signal]");

    let account = vault::get_secret(BLOB_ACCOUNT, &vault_url()).await.unwrap();
    let container = vault::get_secret(BLOB_CONTAINER, &vault_url())
        .await
        .unwrap();
    let key = vault::get_secret(BLOB_KEY, &vault_url()).await.unwrap();

    let local_path = format!("content");

    if let Err(e) = storage::upload_to_blob(&account, &container, &local_path, &key).await {
        println!("Error: upload to blob - {}", e);
    }

    sentry::capture_message("CMDC has finished", Level::Info);
}
