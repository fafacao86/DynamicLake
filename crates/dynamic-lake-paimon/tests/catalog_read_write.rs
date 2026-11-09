use std::error::Error;
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use dynamic_lake_core::{IncrementInterval, RowKind, TableIdentifier};
use dynamic_lake_format::{
    CHANGE_KIND_COLUMN, ChangeReadPlanRequest, CreateDatabaseRequest, CreateTableRequest,
    LakeFormat, ReadPlanRequest, WriteMode, WritePlanRequest,
};
use dynamic_lake_paimon::PaimonFormat;
use paimon::Options;

#[tokio::test]
async fn writes_and_reads_back_through_paimon_format_interface() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let mut options = Options::new();
    options.set(
        "warehouse",
        directory.path().join("warehouse").to_string_lossy(),
    );

    let format = PaimonFormat::from_options(options).await?;
    let catalog = LakeFormat::catalog(&format);
    let planner = LakeFormat::planner(&format);
    catalog
        .create_database(CreateDatabaseRequest {
            database: "analytics".to_string(),
            properties: Vec::new(),
            if_not_exists: false,
        })
        .await?;

    let table: TableIdentifier = "paimon.analytics.events".parse()?;
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    let metadata = catalog
        .create_table(CreateTableRequest {
            table: table.clone(),
            schema,
            partition_by: Vec::new(),
            primary_key: Vec::new(),
            properties: vec![
                ("bucket".to_string(), "1".to_string()),
                ("bucket-key".to_string(), "id".to_string()),
            ],
            comment: Some("DynamicLake Paimon read/write integration test".to_string()),
            ignore_if_exists: false,
        })
        .await?;
    assert_eq!(metadata.latest_version(), None);

    let session = SessionContext::new();
    let input = session
        .sql(
            "SELECT CAST(2 AS BIGINT) AS id, CAST(20 AS BIGINT) AS amount
             UNION ALL
             SELECT CAST(1 AS BIGINT) AS id, CAST(10 AS BIGINT) AS amount",
        )
        .await?
        .create_physical_plan()
        .await?;
    let state = session.state();
    let write = planner
        .plan_write(
            &state,
            WritePlanRequest {
                table: table.clone(),
                input,
                mode: WriteMode::Append,
                expected_snapshot: Some(-1),
            },
        )
        .await?;
    assert_eq!(write.commit_authority, "paimon-table-commit");
    collect(write.execution, session.task_ctx()).await?;

    let metadata = catalog.get_table(&table).await?;
    assert!(
        metadata.latest_version().is_some(),
        "write should advance the Paimon snapshot"
    );

    let read = planner
        .plan_read(
            &state,
            ReadPlanRequest {
                table: table.clone(),
                snapshot: metadata.latest_version(),
                projection: Some(vec![0, 1]),
                filters: Vec::new(),
                limit: None,
            },
        )
        .await?;
    let batches = collect(read.execution, session.task_ctx()).await?;

    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column should be Int64");
        let amounts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("amount column should be Int64");
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row), amounts.value(row)));
        }
    }
    rows.sort_unstable();

    assert_eq!(rows, vec![(1, 10), (2, 20)]);

    let snapshot = metadata
        .latest_version()
        .expect("write produced a snapshot");
    let changes = planner
        .plan_changes(
            &state,
            ChangeReadPlanRequest {
                table,
                interval: IncrementInterval::try_new(snapshot - 1, snapshot)?,
            },
        )
        .await?;
    let batches = collect(changes.execution, session.task_ctx()).await?;
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        2
    );
    for batch in batches {
        assert_eq!(batch.schema().field(0).name(), CHANGE_KIND_COLUMN);
        let kinds = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("change kind should be Int32");
        for row in 0..batch.num_rows() {
            assert_eq!(kinds.value(row), RowKind::Positive.code());
        }
    }
    Ok(())
}
