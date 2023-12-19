use csv::Reader;
use std::error::Error;
use std::path::Path;

fn main() {
    let id = "1-2";
    match find_entry_by_id(id) {
        Ok(entry) => println!("Entry: {:?}", entry),
        Err(e) => println!("Error: {}", e),
    }
}

fn find_entry_by_id(id: &str) -> Result<[Option<String>; 1], Box<dyn Error>> {
    let path = Path::new("D:/repo/Stock/Seasonal-Stock/dataset/CVE/data/CVE_output.csv");
    let mut rdr = Reader::from_path(path)?;
    for result in rdr.records() {
        let record = result?;
        if record.get(0) == Some(id) {
            println!("{:?}", record);
            return Ok([record.get(0).map(|s| s.to_owned())]);
        }
    }
    Err(From::from("No entry found"))
}
