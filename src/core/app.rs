use chrono::{Datelike, Utc};
use sentry::Level;
use std::fs::create_dir_all;
use std::path::Path;

use crate::helpers::config::vault_url;
use crate::helpers::config::MARKETDATA_ADDRESS;

use crate::core::crystal;
use crate::helpers::assets;
use crate::helpers::config::MARKETDATA_PW;
use crate::helpers::config::MARKETDATA_UN;
use crate::helpers::quotes;
use crate::helpers::unzip;
use crate::helpers::vault;

pub async fn run() {
    sentry::capture_message("CMDC is running", Level::Info);
    let current_year = Utc::now().year();
    let quotes_url = format!(
        "https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A{}.ZIP",
        current_year
    );
    let resources_dir = Path::new("./resources");
    let content_dir = Path::new("./content");
    let quotes_path = resources_dir.join(format!("COTAHIST_A{}.ZIP", current_year));
    let unzipped_quotes = resources_dir.join(format!("COTAHIST_A{}.TXT", current_year));
    let assets_file = resources_dir.join(format!("assets-{}.txt", current_year));

    if let Err(e) = create_dir_all(&resources_dir) {
        let error_message = format!(
            "Error: create resources directory {} - {}",
            resources_dir.display(),
            e
        );
        sentry::capture_error(&Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            error_message.clone(),
        )));
        eprintln!("{}", error_message);
        return;
    }

    if let Err(e) = create_dir_all(&content_dir) {
        let error_message = format!(
            "Error: create content directory {} - {}",
            resources_dir.display(),
            e
        );
        sentry::capture_error(&Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            error_message.clone(),
        )));
        return;
    }

    // TODO: FIX EDGE CASE: xxxx-01-01
    if quotes::download(&quotes_url, &quotes_path).await.is_err() {
        sentry::capture_error(&Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Error: download quotes",
        )));
        return;
    }

    unzip::unzip_path(&quotes_path, &resources_dir);

    if let Err(e) = unzip::process_assets(&unzipped_quotes, &assets_file) {
        let error_message = format!("Error: {}", e);
        sentry::capture_error(&Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            error_message.clone(),
        )));
        eprintln!("{}", error_message);
        return;
    }

    println!("Requesting secrets...");
    let mkt_data_address = vault::get_secret(MARKETDATA_ADDRESS, &vault_url())
        .await
        .unwrap();
    let mkt_data_password = vault::get_secret(MARKETDATA_UN, &vault_url())
        .await
        .unwrap()
        + "\n";
    let mkt_data_username = vault::get_secret(MARKETDATA_PW, &vault_url())
        .await
        .unwrap()
        + "\n";
    println!("Secrets received");

    match assets::read_asset_names(assets_file.to_str().unwrap()) {
        Ok(asset_names) => {
            println!("Unique assets: {}", asset_names.len());
            if let Err(e) = crystal::start(
                asset_names,
                mkt_data_address,
                mkt_data_password,
                mkt_data_username,
            )
            .await
            {
                let error_message = format!("Crystal error: {}", e);
                sentry::capture_error(&Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    error_message.clone(),
                )));
                eprintln!("{}", error_message);
            }
        }
        Err(e) => {
            let error_message = format!("Error: read asset names - {}", e);
            sentry::capture_error(&Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                error_message.clone(),
            )));
            eprintln!("{}", error_message);
        }
    }
}
