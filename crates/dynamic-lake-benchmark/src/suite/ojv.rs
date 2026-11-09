use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Array, Int32Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use dynamic_lake_client::PipelineClient;
use dynamic_lake_core::{IncrementInterval, IvmStreamObject, RowKind, TableIdentifier};
use dynamic_lake_datafusion::ivm::{IvmRefreshContext, normalize::ROW_KIND_COLUMN};
use dynamic_lake_format::{
    CreateDatabaseRequest, CreateTableRequest, LakeFormat, LakeFormatRegistry,
};
use dynamic_lake_paimon::PaimonFormat;
use paimon::Options;

use crate::tools::{register_snapshot_table, version, write_rows};

type Rows = BTreeMap<(i64, Option<i64>), i64>;

pub async fn run(row_count: i64, change_count: usize) -> Result<(), Box<dyn Error>> {
    if row_count < 1 || change_count > ((row_count + 1) / 2) as usize {
        return Err("expected rows > 0 and changes <= number of odd rows".into());
    }

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

    LakeFormat::catalog(format.as_ref())
        .create_database(CreateDatabaseRequest {
            database: "analytics".to_string(),
            properties: Vec::new(),
            if_not_exists: false,
        })
        .await?;
    let a_schema = Arc::new(Schema::new(vec![
        Field::new("__ivm_row_id", DataType::Int64, false),
        Field::new("id", DataType::Int64, false),
    ]));
    let b_schema = Arc::new(Schema::new(vec![
        Field::new("__ivm_row_id", DataType::Int64, false),
        Field::new("a_id", DataType::Int64, false),
    ]));
    for name in ["a", "b"] {
        let table: TableIdentifier = format!("paimon.analytics.{name}").parse()?;
        LakeFormat::catalog(format.as_ref())
            .create_table(CreateTableRequest {
                table,
                schema: Arc::clone(if name == "a" { &a_schema } else { &b_schema }),
                partition_by: Vec::new(),
                primary_key: vec!["__ivm_row_id".to_string()],
                properties: vec![
                    ("bucket".to_string(), "1".to_string()),
                    ("bucket-key".to_string(), "__ivm_row_id".to_string()),
                ],
                comment: None,
                ignore_if_exists: false,
            })
            .await?;
    }

    let a: TableIdentifier = "paimon.analytics.a".parse()?;
    let b: TableIdentifier = "paimon.analytics.b".parse()?;
    write_rows(&client, format.as_ref(), &a, 1..=row_count, &a_schema).await?;
    write_rows(
        &client,
        format.as_ref(),
        &b,
        (1..=row_count).filter(|id| id % 2 == 0),
        &b_schema,
    )
    .await?;
    register_snapshot_table(&client, format.as_ref(), &a, &a_schema).await?;
    register_snapshot_table(&client, format.as_ref(), &b, &b_schema).await?;

    let sql = "SELECT paimon.analytics.a.id AS id, paimon.analytics.b.a_id AS b_id \
               FROM paimon.analytics.a LEFT JOIN paimon.analytics.b \
               ON paimon.analytics.a.id = paimon.analytics.b.a_id";
    let view: TableIdentifier = "paimon.analytics.mv".parse()?;
    let definitions = client
        .materialized_view(view.to_string(), client.plan_sql(sql).await?)?
        .build()?;
    let compiled = client.compile(&definitions)?;
    assert!(compiled.node(&view).is_some());

    let before = visible_rows(client.session().sql(sql).await?.collect().await?)?;
    let a_version = version(format.as_ref(), &a).await?;
    let before_b_version = version(format.as_ref(), &b).await?;
    let b_stream = IvmStreamObject::create(view.clone(), b.clone(), Some(before_b_version));
    write_rows(
        &client,
        format.as_ref(),
        &b,
        (1..=row_count).filter(|id| id % 2 == 1).take(change_count),
        &b_schema,
    )
    .await?;
    let after_b_version = version(format.as_ref(), &b).await?;
    let b_interval = b_stream
        .pending_interval(after_b_version)?
        .ok_or("expected a changed source")?;

    let refresh = IvmRefreshContext::new(
        b.clone(),
        BTreeMap::from([
            (a, IncrementInterval::try_new(a_version, a_version)?),
            (b.clone(), b_interval),
        ]),
    )?;
    let started = Instant::now();
    let delta = client
        .plan_incremental(&definitions, &view, refresh)
        .await?
        .ok_or("expected an incremental plan")?;
    let execution = client
        .session()
        .state()
        .create_physical_plan(&delta)
        .await?;
    let delta_batches = collect(execution, client.session().task_ctx()).await?;
    let incremental_time = started.elapsed();

    let started = Instant::now();
    register_snapshot_table(&client, format.as_ref(), &b, &b_schema).await?;
    let recomputed = visible_rows(client.session().sql(sql).await?.collect().await?)?;
    let full_time = started.elapsed();
    let mut maintained = before;
    let delta_rows = apply_delta(&mut maintained, &delta_batches)?;
    assert_eq!(
        maintained, recomputed,
        "incremental result differs from full recompute"
    );
    println!(
        "rows={row_count} changes={change_count} delta_rows={delta_rows} incremental={incremental_time:?} full={full_time:?} result_rows={}",
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
