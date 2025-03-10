use azure_storage::prelude::*;
use azure_storage_blobs::prelude::*;
use chrono::Utc;
use std::error::Error;
use std::fs::{read_dir, remove_dir_all, remove_file, File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::Path;
use tokio::time::{sleep, timeout};
use zip::write::FileOptions;
use zip::CompressionMethod;
use zip::ZipWriter;

use crate::helpers::config::{RETRY_COUNT, RETRY_DELAY, UPLOAD_TIMEOUT_DURATION};

pub async fn upload_to_blob(
    account: &str,
    container: &str,
    file_path: &str,
    access_key: &str,
) -> Result<(), Box<dyn Error>> {
    let zip_file_path = zip_md_folder(file_path)?;
    for attempt in 0..RETRY_COUNT {
        match timeout(
            UPLOAD_TIMEOUT_DURATION * attempt.try_into().unwrap(),
            upload_blob(
                &zip_file_path,
                account,
                container,
                file_path,
                access_key.to_string(),
            ),
        )
        .await
        {
            Ok(Ok(())) => {
                remove_dir_all(file_path)?;
                return Ok(());
            }
            Ok(Err(e)) => {
                eprintln!(
                    "Error: attempt {} to upload blob {} - {}",
                    attempt + 1,
                    file_path,
                    e
                );
            }
            Err(_) => {
                eprintln!(
                    "Error: attempt {} to upload blob {} - timeout",
                    attempt + 1,
                    file_path
                );
            }
        }
        if attempt < RETRY_COUNT - 1 {
            sleep(RETRY_DELAY * attempt.try_into().unwrap()).await;
        }
    }
    Err(format!(
        "Error: attempt {} to upload blob {}",
        RETRY_COUNT, file_path
    )
    .into())
}

async fn upload_blob(
    zip_file_path: &String,
    account: &str,
    container: &str,
    file_path: &str,
    access_key: String,
) -> Result<(), Box<dyn Error>> {
    println!("Sending {} to the Blob Storage", file_path);

    let blob_name = &zip_file_path;
    let date = Utc::now().format("%Y-%m-%d").to_string();

    let storage_credentials = StorageCredentials::access_key(account.to_string(), access_key);
    let blob_client = ClientBuilder::new(account, storage_credentials)
        .blob_client(container, date + "/" + blob_name);

    let mut file = File::open(&zip_file_path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;

    match blob_client
        .put_block_blob(buffer)
        .content_type("application/zip")
        .await
    {
        Ok(_) => {
            println!("File {} sent to the Blob Storage", zip_file_path);
            remove_file(&zip_file_path)?;
            println!("Removed file: {}", zip_file_path);
            Ok(())
        }
        Err(e) => {
            eprintln!("Error: upload blob: {:?}", e);
            remove_file(&zip_file_path)?;
            println!("Removed file: {}", zip_file_path);
            Err(Box::new(e))
        }
    }
}

// TODO: Zip files individually to reduce the chance of ZIP64 corruption
fn zip_md_folder(folder_path: &str) -> Result<String, Box<dyn Error>> {
    let path = Path::new(folder_path);
    let date = Utc::now().format("%Y-%m-%d").to_string();
    let zip_file_name = format!("{}-{}.zip", "md", date);
    let zip_file_path = format!("./{}", zip_file_name);

    let zip_file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(&zip_file_path)?;
    let mut zip = ZipWriter::new(zip_file);
    let options: FileOptions<()> =
        FileOptions::default().compression_method(CompressionMethod::Deflated);

    let buffer_size = 4096;
    let mut buffer = vec![0; buffer_size];

    for entry in read_dir(folder_path)? {
        let entry = entry?;
        let file_path = entry.path();
        if file_path.is_file() {
            let file = File::open(&file_path)?;
            let mut reader = BufReader::new(file);

            let file_name = file_path
                .strip_prefix(&path)?
                .to_str()
                .ok_or("Error: convert file path to str")?;
            zip.start_file(file_name, options)?;

            loop {
                let bytes_read = reader.read(&mut buffer)?;
                if bytes_read == 0 {
                    break;
                }
                zip.write_all(&buffer[..bytes_read])?;
            }
        }
    }

    zip.finish()?;
    Ok(zip_file_path)
}
