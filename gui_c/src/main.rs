use csv::Reader;
use std::error::Error;
use std::path::Path;
use chrono::offset::Local;
use chrono::Datelike;

fn main() {
    let id = &get_today_date();
    let entry = find_entry_by_id(id);
    println!("{:?}", entry);
    // if there is no entry, display no recommendation
    let entry = entry.unwrap_or([None]);
    // print to desktop
    
}

fn find_entry_by_id(id: &str) -> Result<[Option<String>; 1], Box<dyn Error>> {
    let path = Path::new("D:/repo/Stock/Seasonal-Stock/dataset/seasonal_top_performer.csv");
    let mut rdr = Reader::from_path(path)?;
    for result in rdr.records() {
        let record = result?;
        println!("{:?}", record.get(0));
        if record.get(0) == Some(id) {
            println!("{:?}", record);
            return Ok([record.get(0).map(|s| s.to_owned())]);
        }
    }
    Err(From::from("No entry found"))
}

fn get_today_date() -> String {
    // Get today's date in format: week number, month
    let today = Local::now();
    let week_number = today.day() / 7 + 2;
    // format week_number to a string as '1', '2', '33'...
    let week_number = week_number.to_string();
    // get short month in format '1' for January, '22' for February...
    let month = today.month().to_string();
    let date = format!("{}-{}", month, week_number);
    date
}