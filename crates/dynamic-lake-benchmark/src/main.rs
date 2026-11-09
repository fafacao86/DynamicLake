mod suite;
mod tools;

use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    let suite = args.get(1).map(String::as_str).unwrap_or("ojv");
    let rows = args
        .get(2)
        .map_or(Ok(1_000), |value| value.parse::<i64>())?;
    let changes = args
        .get(3)
        .map_or(Ok(100), |value| value.parse::<usize>())?;
    suite::run(suite, rows, changes).await
}
