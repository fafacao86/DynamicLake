use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::prelude::SessionContext;
use dynamic_lake_client::{InProcessPipelineClient, PipelineClient};
use dynamic_lake_core::{PipelineError, TableIdentifier};
use dynamic_lake_format::{CreateDatabaseRequest, CreateTableRequest, LakeFormatRegistry};
use dynamic_lake_metastore_redb::RedbMetastoreFactory;
use dynamic_lake_paimon::{PaimonFormat, table_options};
use paimon::Options;

use crate::tools::{tpch_dataset::tpch_dataset, write_rows};

const OJV_SQL: &str = include_str!("../../sql/ojv.sql");
const DOWNSTREAM_SQL: &str = include_str!("../../sql/ojv_downstream.sql");
const THIRD_SQL: &str = include_str!("../../sql/ojv_third.sql");

type Rows = BTreeMap<(i64, Option<i64>), i64>;

pub async fn run(row_count: i64, change_count: usize) -> Result<(), Box<dyn Error>> {
    let dataset = tpch_dataset(row_count, change_count)?;

    let warehouse = tempfile::tempdir()?;
    let mut options = Options::new();
    options.set(
        "warehouse",
        warehouse.path().join("warehouse").to_string_lossy(),
    );
    let format = Arc::new(PaimonFormat::from_options(options).await?);
    let registry = LakeFormatRegistry::new().register("paimon", format.clone());
    let session = SessionContext::new();
    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog.register_schema("analytics", Arc::new(MemorySchemaProvider::new()))?;
    session.register_catalog("paimon", catalog);
    let metadata_path = warehouse.path().join("metadata.redb");
    let client = PipelineClient::new(Arc::new(
        InProcessPipelineClient::new(
            session.clone(),
            registry.clone(),
            &metadata_path,
            &RedbMetastoreFactory,
        )
        .await?,
    ));

    client
        .create_database(
            "paimon",
            CreateDatabaseRequest {
                database: "analytics".to_string(),
                properties: Vec::new(),
                if_not_exists: false,
            },
        )
        .await?;
    let a_schema = Arc::new(Schema::new(vec![
        Field::new("__ivm_row_id", DataType::Int64, false),
        Field::new("customer_key", DataType::Int64, false),
    ]));
    let b_schema = Arc::new(Schema::new(vec![
        Field::new("__ivm_row_id", DataType::Int64, false),
        Field::new("order_customer_key", DataType::Int64, false),
    ]));
    for name in ["a", "b"] {
        let table: TableIdentifier = format!("paimon.analytics.{name}").parse()?;
        client
            .create_table(CreateTableRequest {
                table,
                schema: Arc::clone(if name == "a" { &a_schema } else { &b_schema }),
                partition_by: Vec::new(),
                primary_key: vec!["__ivm_row_id".to_string()],
                properties: vec![
                    (table_options::BUCKET.to_string(), "1".to_string()),
                    (
                        table_options::BUCKET_KEY.to_string(),
                        "__ivm_row_id".to_string(),
                    ),
                    (
                        table_options::FILE_FORMAT.to_string(),
                        "parquet".to_string(),
                    ),
                ],
                comment: None,
                ignore_if_exists: false,
            })
            .await?;
    }

    let a: TableIdentifier = "paimon.analytics.a".parse()?;
    let b: TableIdentifier = "paimon.analytics.b".parse()?;
    write_rows(&client, &a, dataset.customers, &a_schema).await?;
    write_rows(&client, &b, dataset.initial_orders, &b_schema).await?;
    client.register_table_snapshot(&a).await?;
    client.register_table_snapshot(&b).await?;

    let view: TableIdentifier = "paimon.analytics.mv".parse()?;
    client
        .materialized_view(view.to_string(), OJV_SQL)
        .create()
        .await?;
    assert!(
        matches!(
            client.append_batches(&view, Vec::new()).await,
            Err(PipelineError::Invalid(_))
        ),
        "client writes to a materialized view must be rejected"
    );
    client.trigger_incremental_refresh().await?;
    client.register_table_snapshot(&view).await?;

    let downstream: TableIdentifier = "paimon.analytics.mv_downstream".parse()?;
    client
        .materialized_view(downstream.to_string(), DOWNSTREAM_SQL)
        .create()
        .await?;
    client.trigger_incremental_refresh().await?;
    client.register_table_snapshot(&downstream).await?;
    let third: TableIdentifier = "paimon.analytics.mv_third".parse()?;
    client
        .materialized_view(third.to_string(), THIRD_SQL)
        .create()
        .await?;
    client.trigger_incremental_refresh().await?;
    let downstream_before = visible_rows(client.query_sql(DOWNSTREAM_SQL).await?)?;

    let before = visible_rows(client.query_sql(OJV_SQL).await?)?;
    write_rows(&client, &b, dataset.new_orders, &b_schema).await?;
    let upstream_version = client.latest_version(&view).await?;
    let downstream_version = client.latest_version(&downstream).await?;
    let third_version = client.latest_version(&third).await?;
    let started = Instant::now();
    client.trigger_incremental_refresh().await?;
    let incremental_time = started.elapsed();
    let refreshed_upstream_version = client.latest_version(&view).await?;
    let refreshed_downstream_version = client.latest_version(&downstream).await?;
    let refreshed_third_version = client.latest_version(&third).await?;
    assert!(
        refreshed_upstream_version > upstream_version,
        "incremental refresh did not commit the upstream MV"
    );
    assert!(
        refreshed_downstream_version > downstream_version,
        "incremental refresh did not propagate to the downstream MV"
    );
    assert!(
        refreshed_third_version > third_version,
        "incremental refresh did not propagate through the third MV"
    );
    client.trigger_incremental_refresh().await?;
    assert_eq!(
        client.latest_version(&view).await?,
        refreshed_upstream_version,
        "the source cursor was not advanced after the MV commit"
    );
    assert_eq!(
        client.latest_version(&downstream).await?,
        refreshed_downstream_version,
        "the downstream cursor was not advanced after the propagated refresh"
    );
    assert_eq!(
        client.latest_version(&third).await?,
        refreshed_third_version
    );
    let started = Instant::now();
    client.register_table_snapshot(&b).await?;
    let recomputed = visible_rows(client.query_sql(OJV_SQL).await?)?;
    let full_time = started.elapsed();
    assert_ne!(before, recomputed, "source append produced no view changes");
    client.register_table_snapshot(&view).await?;
    let persisted_upstream = visible_rows(
        client
            .query_sql("SELECT id, b_id FROM paimon.analytics.mv")
            .await?,
    )?;
    assert_eq!(
        persisted_upstream, recomputed,
        "persisted upstream MV differs from full recompute"
    );
    let downstream_recomputed = visible_rows(client.query_sql(DOWNSTREAM_SQL).await?)?;
    assert_ne!(
        downstream_before, downstream_recomputed,
        "MV-on-MV produced no changes"
    );
    client.register_table_snapshot(&downstream).await?;
    let persisted_downstream = visible_rows(
        client
            .query_sql("SELECT id, b_id FROM paimon.analytics.mv_downstream")
            .await?,
    )?;
    assert_eq!(
        persisted_downstream, downstream_recomputed,
        "persisted downstream MV differs from full recompute"
    );
    client.register_table_snapshot(&third).await?;
    assert_eq!(
        visible_rows(
            client
                .query_sql("SELECT id, b_id FROM paimon.analytics.mv_third")
                .await?
        )?,
        downstream_recomputed,
        "persisted third MV differs from full recompute"
    );
    drop(client);
    let restarted = PipelineClient::new(Arc::new(
        InProcessPipelineClient::new(session, registry, &metadata_path, &RedbMetastoreFactory)
            .await?,
    ));
    assert!(
        matches!(
            restarted.append_batches(&view, Vec::new()).await,
            Err(PipelineError::Invalid(_))
        ),
        "restarted server must still reject client writes to the MV"
    );
    restarted.trigger_incremental_refresh().await?;
    assert_eq!(
        restarted.latest_version(&view).await?,
        refreshed_upstream_version,
        "restarted server lost the upstream source cursor"
    );
    assert_eq!(
        restarted.latest_version(&downstream).await?,
        refreshed_downstream_version,
        "restarted server lost the downstream source cursor"
    );
    assert_eq!(
        restarted.latest_version(&third).await?,
        refreshed_third_version
    );
    restarted.trigger_full_refresh().await?;
    assert!(
        restarted.latest_version(&view).await? > refreshed_upstream_version,
        "full refresh did not commit the upstream MV"
    );
    assert!(
        restarted.latest_version(&downstream).await? > refreshed_downstream_version,
        "full refresh did not propagate to the downstream MV"
    );
    assert!(
        restarted.latest_version(&third).await? > refreshed_third_version,
        "full refresh did not propagate through the third MV"
    );
    restarted.register_table_snapshot(&view).await?;
    restarted.register_table_snapshot(&downstream).await?;
    restarted.register_table_snapshot(&third).await?;
    assert_eq!(
        visible_rows(
            restarted
                .query_sql("SELECT id, b_id FROM paimon.analytics.mv")
                .await?
        )?,
        recomputed,
        "full refresh produced stale upstream rows"
    );
    assert_eq!(
        visible_rows(
            restarted
                .query_sql("SELECT id, b_id FROM paimon.analytics.mv_downstream")
                .await?
        )?,
        downstream_recomputed,
        "full refresh produced stale downstream rows"
    );
    assert_eq!(
        visible_rows(
            restarted
                .query_sql("SELECT id, b_id FROM paimon.analytics.mv_third")
                .await?
        )?,
        downstream_recomputed,
        "full refresh produced stale third MV rows"
    );
    println!(
        "rows={row_count} changes={change_count} incremental_chain={incremental_time:?} full={full_time:?} result_rows={}",
        recomputed.values().sum::<i64>()
    );
    Ok(())
}

fn visible_rows(batches: Vec<RecordBatch>) -> Result<Rows, Box<dyn Error>> {
    let mut rows = Rows::new();
    for batch in batches {
        let schema = batch.schema();
        let id = batch
            .column(schema.index_of("id")?)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("id is not Int64")?;
        let b_id = batch
            .column(schema.index_of("b_id")?)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("b_id is not Int64")?;
        for row in 0..batch.num_rows() {
            *rows
                .entry((id.value(row), (!b_id.is_null(row)).then(|| b_id.value(row))))
                .or_default() += 1;
        }
    }
    Ok(rows)
}
