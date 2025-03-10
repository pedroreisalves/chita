use chrono::Datelike;
use chrono::Utc;

pub fn get_futures() -> Vec<String> {
    let current_year = Utc::now().year() % 100;
    // F - January
    // G - February
    // H - March
    // J - April
    // K - May
    // M - June
    // N - July
    // Q - August
    // U - September
    // V - October
    // X - November
    // Z - December
    let letters_win = vec!['G', 'J', 'M', 'Q', 'V', 'Z'];
    let letters_wdo = vec!['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z'];
    let letters_bit = vec!['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z'];

    let mut result = Vec::new();

    for letter in &letters_win {
        result.push(format!("win{}{:02}", letter, current_year));
    }

    for letter in &letters_wdo {
        result.push(format!("wdo{}{:02}", letter, current_year));
    }

    for letter in &letters_bit {
        result.push(format!("bit{}{:02}", letter, current_year));
    }

    result
}
