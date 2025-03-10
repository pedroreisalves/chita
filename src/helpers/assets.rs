use std::collections::HashSet;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};

pub fn extract_asset_names(file_path: &str) -> Result<HashSet<String>, io::Error> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let mut asset_names = HashSet::new();

    for line in reader.lines() {
        let line = line?;
        if line.len() >= 24 {
            let asset_name = line[12..24].trim().to_string();
            asset_names.insert(asset_name);
        }
    }

    Ok(asset_names)
}

pub fn save_asset_names(asset_names: &HashSet<String>, file_path: &str) -> Result<(), io::Error> {
    let mut file = File::create(file_path)?;
    for name in asset_names {
        writeln!(file, "{}", name)?;
    }
    Ok(())
}

pub fn read_asset_names(file_path: &str) -> Result<Vec<String>, io::Error> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let mut asset_names = Vec::new();

    for line in reader.lines() {
        let line = line?;
        asset_names.push(line.trim().to_string());
    }

    Ok(asset_names)
}
