use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Array, Int32Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::prelude::SessionContext;
use dynamic_lake_client::PipelineClient;
use dynamic_lake_core::{
    GraphOperations, IncrementInterval, IvmStreamObject, RowKind, TableIdentifier,
};
use dynamic_lake_datafusion::ivm::{IvmRefreshContext, normalize::ROW_KIND_COLUMN};
use dynamic_lake_format::{CreateDatabaseRequest, CreateTableRequest, LakeFormatRegistry};
use dynamic_lake_paimon::{PaimonFormat, table_options};
use paimon::Options;

use crate::tools::{tpch_dataset::tpch_dataset, write_rows};

const OJV_SQL: &str = include_str!("../../sql/ojv.sql");
const DOWNSTREAM_SQL: &str = include_str!("../../sql/ojv_downstream.sql");

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
    let client = PipelineClient::new(session).with_formats(registry);

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
    let definitions = client
        .materialized_view_sql(view.to_string(), OJV_SQL)
        .await?
        .create()
        .await?;
    client.trigger_full_refresh(&definitions, &view).await?;
    client.register_table_snapshot(&view).await?;

    let downstream: TableIdentifier = "paimon.analytics.mv_downstream".parse()?;
    let downstream_definitions = client
        .materialized_view_sql(downstream.to_string(), DOWNSTREAM_SQL)
        .await?
        .create()
        .await?;
    client
        .trigger_full_refresh(&downstream_definitions, &downstream)
        .await?;
    let chain = client
        .materialized_view(view.to_string(), client.plan_sql(OJV_SQL).await?)?
        .materialized_view(
            downstream.to_string(),
            client.plan_sql(DOWNSTREAM_SQL).await?,
        )?
        .build()?;
    assert!(
        chain
            .graph()
            .node(&downstream)
            .ok_or("missing downstream MV")?
            .upstream
            .contains(&view),
        "MV dependency graph is missing the upstream MV"
    );
    let downstream_before = visible_rows(client.query_sql(DOWNSTREAM_SQL).await?)?;
    let before_mv_version = client.latest_version(&view).await?;
    let mv_stream =
        IvmStreamObject::create(downstream.clone(), view.clone(), Some(before_mv_version));

    let before = visible_rows(client.query_sql(OJV_SQL).await?)?;
    let a_version = client.latest_version(&a).await?;
    let before_b_version = client.latest_version(&b).await?;
    let b_stream = IvmStreamObject::create(view.clone(), b.clone(), Some(before_b_version));
    write_rows(&client, &b, dataset.new_orders, &b_schema).await?;
    let after_b_version = client.latest_version(&b).await?;
    let b_interval = b_stream
        .pending_interval(after_b_version)?
        .ok_or("expected a changed source")?;

    let refresh = IvmRefreshContext::new(
        b.clone(),
        BTreeMap::from([
            (a.clone(), IncrementInterval::try_new(a_version, a_version)?),
            (b.clone(), b_interval),
        ]),
    )?;
    let started = Instant::now();
    let delta_batches = client
        .trigger_incremental_refresh(
            &chain,
            &view,
            chain
                .plan_signature(&view)
                .ok_or("missing plan signature")?,
            refresh,
        )
        .await?
        .ok_or("expected incremental changes")?;
    let incremental_time = started.elapsed();

    let started = Instant::now();
    client.register_table_snapshot(&b).await?;
    let recomputed = visible_rows(client.query_sql(OJV_SQL).await?)?;
    let full_time = started.elapsed();
    let mut maintained = before;
    let delta_rows = apply_delta(&mut maintained, &delta_batches)?;
    assert_eq!(
        maintained, recomputed,
        "incremental result differs from full recompute"
    );
    let after_mv_version = client.latest_version(&view).await?;
    let mv_interval = mv_stream
        .pending_interval(after_mv_version)?
        .ok_or("expected a changed upstream MV")?;
    let downstream_refresh = IvmRefreshContext::new(
        view.clone(),
        BTreeMap::from([
            (a.clone(), IncrementInterval::try_new(a_version, a_version)?),
            (view.clone(), mv_interval),
        ]),
    )?;
    let started = Instant::now();
    let downstream_delta = client
        .trigger_incremental_refresh(
            &chain,
            &downstream,
            chain
                .plan_signature(&downstream)
                .ok_or("missing downstream plan signature")?,
            downstream_refresh,
        )
        .await?
        .ok_or("expected downstream incremental changes")?;
    let downstream_time = started.elapsed();
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
    let mut downstream_maintained = downstream_before;
    let downstream_delta_rows = apply_delta(&mut downstream_maintained, &downstream_delta)?;
    assert!(downstream_delta_rows > 0, "MV-on-MV produced no changes");
    assert_eq!(
        downstream_maintained, downstream_recomputed,
        "MV-on-MV incremental result differs from full recompute"
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
    println!(
        "rows={row_count} changes={change_count} delta_rows={delta_rows} incremental={incremental_time:?} full={full_time:?} downstream_delta_rows={downstream_delta_rows} downstream={downstream_time:?} result_rows={}",
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

fn apply_delta(rows: &mut Rows, batches: &[RecordBatch]) -> Result<usize, Box<dyn Error>> {
    let mut count = 0;
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
        let kind = batch
            .column(schema.index_of(ROW_KIND_COLUMN)?)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("row kind is not Int32")?;
        for row in 0..batch.num_rows() {
            let key = (id.value(row), (!b_id.is_null(row)).then(|| b_id.value(row)));
            let sign = kind.value(row);
            assert!(sign == RowKind::Positive.code() || sign == RowKind::Negative.code());
            *rows.entry(key).or_default() += i64::from(sign);
            count += 1;
        }
    }
    rows.retain(|_, count| *count != 0);
    assert!(rows.values().all(|count| *count > 0));
    Ok(count)
}
