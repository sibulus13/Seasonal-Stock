use chrono::offset::Local;
use chrono::Datelike;
use csv::Reader;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::path::Path;

#[derive(Debug, Serialize, Deserialize)]
struct SeasonalTopPerformer {
    #[serde(rename = "month and week")]
    month_and_week: String,
    #[serde(rename = "top performer 1")]
    top_performer_1: String,
    #[serde(rename = "top performer 2")]
    top_performer_2: String,
    #[serde(rename = "top performer 3")]
    top_performer_3: String,
    #[serde(rename = "top performer 1 gain/loss ratio")]
    top_performer_1_gain_loss_ratio: String,
    #[serde(rename = "top performer 2 gain/loss ratio")]
    top_performer_2_gain_loss_ratio: String,
    #[serde(rename = "top performer 3 gain/loss ratio")]
    top_performer_3_gain_loss_ratio: String,
    #[serde(rename = "num years")]
    num_years: String,
    month: String,
    week: String,
}

struct DateFormat {
    month: u32,
    week: u32,
}

fn main() {
    let date = &get_today_date();
    let entry = find_entry_by_date(date);
    println!("{:?}", entry);
}

fn find_entry_by_date(
    date: &DateFormat,
) -> Result<[Option<SeasonalTopPerformer>; 1], Box<dyn Error>> {
    let path = Path::new("D:/repo/Stock/Seasonal-Stock/dataset/seasonal_top_performer.csv");
    let mut rdr = Reader::from_path(path)?;
    for result in rdr.deserialize() {
        let entry: SeasonalTopPerformer = result?;
        // * I'm using the below block to compare because idk a better way, I would assume setting import directly to i32 would be better?
        // convert date.month to string with 1 sig fig
        let month = f64::from(date.month);
        let month = format!("{:.1}", month);
        // convert date.week to string with 1 sig fig
        let week = f64::from(date.week);
        let week = format!("{:.1}", week);
        if entry.month == month && entry.week == week {
            return Ok([Some(entry)]);
        }
    }
    Err(From::from("No entry found"))
}

fn get_today_date() -> DateFormat {
    // Get today's date in format: week number, month
    let today = Local::now();
    let week_number = today.day() / 7 + 2;
    let month = today.month();
    let month_and_week = DateFormat {
        month: month as u32,
        week: week_number as u32,
    };
    month_and_week
}
