use std::fs::{create_dir_all, File};
use std::io::copy;
use std::path::Path;
use zip::read::ZipArchive;

use crate::helpers::assets;

pub fn unzip_path(zip_path: &Path, dest_dir: &Path) {
    unzip_file(zip_path.to_str().unwrap(), dest_dir.to_str().unwrap());
}

pub fn process_assets(
    unzipped_file_path: &Path,
    asset_names_file_path: &Path,
) -> Result<(), std::io::Error> {
    let asset_names = assets::extract_asset_names(unzipped_file_path.to_str().unwrap())?;
    assets::save_asset_names(&asset_names, asset_names_file_path.to_str().unwrap()).map_err(|e| {
        eprintln!("Error: save asset names - {}", e);
        e
    })
}

fn unzip_file(src: &str, dest: &str) {
    let file = File::open(src).expect("Error: open file");
    let mut archive = ZipArchive::new(file).expect("Error: read file");

    for i in 0..archive.len() {
        let mut file = archive.by_index(i).expect("Error: access file");
        let unz_path = Path::new(dest).join(file.name());

        if file.name().ends_with('/') {
            create_dir_all(&unz_path).expect("Error: create directory");
        } else {
            if let Some(p) = unz_path.parent() {
                if !p.exists() {
                    create_dir_all(&p).expect("Error: create directory");
                }
            }
            let mut outfile = File::create(&unz_path).expect("Error: create file");
            copy(&mut file, &mut outfile).expect("Error: copy file");
        }
    }
    println!("Unzipped to: {}", dest);
}
