use azure_security_keyvault::KeyvaultClient;
use std::error::Error;
use tokio::time::sleep;
use tokio::time::timeout;

use super::config::RETRY_COUNT;
use super::config::RETRY_DELAY;
use super::config::TIMEOUT_DURATION;

pub async fn get_secret(secret_name: &str, vault_url: &str) -> Result<String, Box<dyn Error>> {
    for attempt in 0..RETRY_COUNT {
        match timeout(
            TIMEOUT_DURATION * attempt.try_into().unwrap(),
            get_vault_secret(secret_name, vault_url),
        )
        .await
        {
            Ok(Ok(secret)) => return Ok(secret),
            Ok(Err(e)) => {
                eprintln!(
                    "Error: attempt {} to get secret {} - {}",
                    attempt + 1,
                    secret_name,
                    e
                );
            }
            Err(_) => {
                eprintln!(
                    "Error: attempt {} to get secret {} - timeout",
                    attempt + 1,
                    secret_name
                );
            }
        }
        if attempt < RETRY_COUNT - 1 {
            sleep(RETRY_DELAY * attempt.try_into().unwrap()).await;
        }
    }
    Err(format!(
        "Error: attempt {} to get secret {}",
        RETRY_COUNT, secret_name
    )
    .into())
}

async fn get_vault_secret(secret_name: &str, vault_url: &str) -> Result<String, Box<dyn Error>> {
    println!("Getting: {}", secret_name);
    let credential = azure_identity::create_credential()?;
    println!("{}",vault_url);
    let client = KeyvaultClient::new(vault_url, credential)
        .map_err(|e| format!("Error: create KeyvaultClient: {}", e))?
        .secret_client();
    let secret = client.get(secret_name).await?;
    Ok(secret.value)
}
