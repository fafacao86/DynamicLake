pub mod ojv;

use std::error::Error;

pub async fn run(name: &str, rows: i64, changes: usize) -> Result<(), Box<dyn Error>> {
    match name {
        "ojv" => ojv::run(rows, changes).await,
        _ => Err(format!("unknown benchmark suite `{name}`").into()),
    }
}
