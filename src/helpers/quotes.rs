use reqwest::Client;
use std::fs::File;
use std::io::copy;
use std::path::Path;
use std::time::Duration;
use tokio::time::sleep;

const MAX_RETRIES: u8 = 30;

pub async fn download(url: &str, dest_path: &Path) -> Result<(), reqwest::Error> {
    download_assets(url, dest_path.to_str().unwrap())
        .await
        .map_err(|e| {
            eprintln!("Error: {}", e);
            e
        })
}

async fn download_assets(url: &str, dest: &str) -> Result<(), reqwest::Error> {
    let client = Client::builder()
        .danger_accept_invalid_certs(true)
        .build()?;

    let mut retries = 0;
    while retries < MAX_RETRIES {
        match client.get(url).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    let mut dest_file = File::create(dest).expect("Error: create file");
                    let content = response.bytes().await.expect("Error: retrieve data");
                    copy(&mut content.as_ref(), &mut dest_file).expect("Error: copy data");
                    println!("File downloaded: {}", dest);
                    return Ok(());
                } else {
                    println!("Error: {}", response.status());
                }
            }
            Err(e) => {
                println!("Error: {}", e);
            }
        }
        retries += 1;
        println!("Retrying - [{}/{}]", retries, MAX_RETRIES);
        sleep(Duration::from_secs(2)).await;
    }
    println!("Error: download file - attempt {}", MAX_RETRIES);
    Ok(())
}
