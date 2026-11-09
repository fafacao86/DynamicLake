use std::error::Error;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::Schema;
use dynamic_lake_client::PipelineClient;
use dynamic_lake_core::TableIdentifier;

pub mod tpch_dataset;

pub async fn write_rows(
    client: &PipelineClient,
    table: &TableIdentifier,
    rows: impl IntoIterator<Item = (i64, i64)>,
    schema: &Arc<Schema>,
) -> Result<(), Box<dyn Error>> {
    let (ids, values): (Vec<_>, Vec<_>) = rows.into_iter().unzip();
    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )?;
    client.append_batches(table, vec![batch]).await?;
    Ok(())
}
