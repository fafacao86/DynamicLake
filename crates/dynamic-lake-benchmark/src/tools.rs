use std::error::Error;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::Schema;
use datafusion::common::TableReference;
use datafusion::datasource::MemTable;
use datafusion::physical_plan::collect;
use dynamic_lake_client::PipelineClient;
use dynamic_lake_core::TableIdentifier;
use dynamic_lake_format::{LakeFormat, ReadPlanRequest, WriteMode, WritePlanRequest};
use dynamic_lake_paimon::PaimonFormat;

pub async fn write_rows(
    client: &PipelineClient,
    format: &PaimonFormat,
    table: &TableIdentifier,
    ids: impl IntoIterator<Item = i64>,
    schema: &Arc<Schema>,
) -> Result<(), Box<dyn Error>> {
    let ids = ids.into_iter().collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(Int64Array::from(ids)),
        ],
    )?;
    let session = client.session();
    session.register_table(
        "__benchmark_input",
        Arc::new(MemTable::try_new(Arc::clone(schema), vec![vec![batch]])?),
    )?;
    let input = session
        .sql("SELECT * FROM __benchmark_input")
        .await?
        .create_physical_plan()
        .await?;
    let expected_snapshot = LakeFormat::catalog(format)
        .get_table(table)
        .await?
        .latest_version()
        .or(Some(-1));
    let state = session.state();
    let write = LakeFormat::planner(format)
        .plan_write(
            &state,
            WritePlanRequest {
                table: table.clone(),
                input,
                mode: WriteMode::Append,
                expected_snapshot,
            },
        )
        .await?;
    collect(write.execution, session.task_ctx()).await?;
    session.deregister_table("__benchmark_input")?;
    Ok(())
}

pub async fn register_snapshot_table(
    client: &PipelineClient,
    format: &PaimonFormat,
    table: &TableIdentifier,
    schema: &Arc<Schema>,
) -> Result<(), Box<dyn Error>> {
    let reference = TableReference::full("paimon", "analytics", table.table());
    let _ = client.session().deregister_table(reference.clone());
    let state = client.session().state();
    let read = LakeFormat::planner(format)
        .plan_read(
            &state,
            ReadPlanRequest {
                table: table.clone(),
                snapshot: None,
                projection: None,
                filters: Vec::new(),
                limit: None,
            },
        )
        .await?;
    let batches = collect(read.execution, client.session().task_ctx())
        .await?
        .into_iter()
        .map(|batch| RecordBatch::try_new(Arc::clone(schema), batch.columns().to_vec()))
        .collect::<Result<Vec<_>, _>>()?;
    client.session().register_table(
        reference,
        Arc::new(MemTable::try_new(Arc::clone(schema), vec![batches])?),
    )?;
    Ok(())
}

pub async fn version(
    format: &PaimonFormat,
    table: &TableIdentifier,
) -> Result<i64, Box<dyn Error>> {
    Ok(LakeFormat::catalog(format)
        .get_table(table)
        .await?
        .latest_version()
        .ok_or("missing snapshot")?)
}
