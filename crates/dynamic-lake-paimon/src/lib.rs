//! Apache Paimon implementation of the DynamicLake lake-format SPI.
//!
//! DAG and refresh code never imports Paimon types. The format registry talks
//! to this crate through `LakeCatalog` and `LakeTablePlanner`.

mod change_read;
mod change_sink;
pub mod table_options;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::ScalarValue;
use datafusion::datasource::TableProvider;
use datafusion::datasource::sink::DataSinkExec;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::expressions::{CaseExpr, Column, Literal};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use dynamic_lake_core::{
    IVM_ROW_KIND_COLUMN, PipelineError, PipelineResult, RowKind, TableIdentifier,
};
use dynamic_lake_format::{
    AlterTableRequest, ChangeReadPlanRequest, CreateDatabaseRequest, CreateTableRequest,
    DropDatabaseRequest, DropTableRequest, FormatCapability, LakeCatalog, LakeFormat,
    LakeFormatError, LakeFormatKind, LakeFormatResult, LakeTable, LakeTablePlanner, ReadPlan,
    ReadPlanRequest, ScanAuthority, TableAlteration, WriteMode, WritePlan, WritePlanRequest,
};
use paimon::catalog::{Catalog, Identifier};
use paimon::spec::{RowKind as PaimonRowKind, Schema, SchemaChange, VALUE_KIND_FIELD_NAME};
use paimon::{Options, Table};
use paimon_datafusion::{PaimonTableProvider, SQLContext};

use crate::change_sink::PaimonChangeSink;

const PAIMON_CAPABILITIES: &[FormatCapability] = &[
    FormatCapability::CreateTable,
    FormatCapability::AlterTable,
    FormatCapability::DropTable,
    FormatCapability::SnapshotVersion,
    FormatCapability::FullScan,
    FormatCapability::IncrementalScan,
    FormatCapability::Overwrite,
    FormatCapability::Append,
    FormatCapability::CatalogCommit,
];

#[derive(Clone)]
pub struct PaimonFormat {
    catalog: Arc<dyn Catalog>,
}

impl PaimonFormat {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }

    pub async fn from_options(options: Options) -> PipelineResult<Self> {
        let catalog = paimon::CatalogFactory::create(options)
            .await
            .map_err(|error| PipelineError::Catalog(error.to_string()))?;
        Ok(Self::new(catalog))
    }

    pub fn catalog(&self) -> &Arc<dyn Catalog> {
        &self.catalog
    }

    async fn metadata(&self, table: &TableIdentifier) -> LakeFormatResult<Arc<dyn LakeTable>> {
        let identifier = paimon_identifier(table.parts())?;
        let table_path = table.clone();
        let table = self
            .catalog
            .get_table(&identifier)
            .await
            .map_err(catalog_error)?;
        table_metadata(table, table_path).await
    }
}

/// Apply the Paimon settings required by a managed materialized view table.
pub fn normalize_managed_table_options(properties: Vec<(String, String)>) -> Vec<(String, String)> {
    let mut options = Options::from_map(properties.into_iter().collect());
    for (key, value) in [
        (table_options::WRITE_ONLY, false),
        (table_options::COMMIT_FORCE_COMPACT, true),
        (table_options::COMPACTION_FORCE_UP_LEVEL_0, true),
        (table_options::WRITE_BUFFER_SPILLABLE, false),
        (table_options::DELETION_VECTORS_ENABLED, true),
        (table_options::LOOKUP_REMOTE_FILE_ENABLED, true),
    ] {
        options.set(key, value.to_string());
    }
    let mut options = options
        .to_map()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    options.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    options
}

#[async_trait]
impl LakeFormat for PaimonFormat {
    fn name(&self) -> &str {
        "paimon"
    }

    fn kind(&self) -> LakeFormatKind {
        LakeFormatKind::Paimon
    }

    fn capabilities(&self) -> &[FormatCapability] {
        PAIMON_CAPABILITIES
    }

    fn catalog(&self) -> &dyn LakeCatalog {
        self
    }

    fn planner(&self) -> &dyn LakeTablePlanner {
        self
    }

    async fn get_identity_columns(&self, table: &TableIdentifier) -> LakeFormatResult<Vec<String>> {
        let identifier = paimon_identifier(table.parts())?;
        let paimon_table = self
            .catalog
            .get_table(&identifier)
            .await
            .map_err(catalog_error)?;
        let schema = paimon_table.schema();
        if schema
            .fields()
            .iter()
            .any(|field| field.name() == "__ivm_row_id")
        {
            return Ok(vec!["__ivm_row_id".to_string()]);
        }
        if schema.primary_keys().is_empty() {
            return Err(LakeFormatError::Unsupported(format!(
                "table `{table}` has neither a persisted row identity nor a primary key"
            )));
        }
        Ok(schema.primary_keys().to_vec())
    }
}

#[async_trait]
impl LakeCatalog for PaimonFormat {
    async fn create_database(&self, request: CreateDatabaseRequest) -> LakeFormatResult<()> {
        self.catalog
            .create_database(
                &request.database,
                request.if_not_exists,
                request.properties.into_iter().collect(),
            )
            .await
            .map_err(catalog_error)
    }

    async fn drop_database(&self, request: DropDatabaseRequest) -> LakeFormatResult<()> {
        self.catalog
            .drop_database(&request.database, request.if_exists, request.cascade)
            .await
            .map_err(catalog_error)
    }

    async fn get_table(&self, table: &TableIdentifier) -> LakeFormatResult<Arc<dyn LakeTable>> {
        self.metadata(table).await
    }

    async fn create_table(
        &self,
        request: CreateTableRequest,
    ) -> LakeFormatResult<Arc<dyn LakeTable>> {
        let identifier = paimon_identifier(request.table.parts())?;
        let table_path = request.table.clone();
        let fields = paimon::arrow::arrow_fields_to_paimon(
            &request
                .schema
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect::<Vec<_>>(),
        )
        .map_err(catalog_error)?;
        let mut builder = Schema::builder();
        for field in fields {
            builder = builder.column_with_description(
                field.name().to_string(),
                field.data_type().clone(),
                field.description().map(str::to_string),
            );
        }
        let options = request
            .properties
            .into_iter()
            .collect::<HashMap<String, String>>();
        let schema = builder
            .partition_keys(request.partition_by)
            .primary_key(request.primary_key)
            .options(options)
            .comment(request.comment)
            .build()
            .map_err(catalog_error)?;
        self.catalog
            .create_table(&identifier, schema, request.ignore_if_exists)
            .await
            .map_err(catalog_error)?;
        let table = self
            .catalog
            .get_table(&identifier)
            .await
            .map_err(catalog_error)?;
        table_metadata(table, table_path).await
    }

    async fn create_materialized_view_table(
        &self,
        table: TableIdentifier,
        schema: datafusion::arrow::datatypes::SchemaRef,
    ) -> LakeFormatResult<Arc<dyn LakeTable>> {
        self.create_table(CreateTableRequest {
            table,
            schema,
            partition_by: Vec::new(),
            primary_key: vec!["__ivm_row_id".to_string()],
            properties: normalize_managed_table_options(vec![
                (table_options::BUCKET.to_string(), "1".to_string()),
                (
                    table_options::BUCKET_KEY.to_string(),
                    "__ivm_row_id".to_string(),
                ),
                (
                    table_options::FILE_FORMAT.to_string(),
                    "parquet".to_string(),
                ),
            ]),
            comment: None,
            ignore_if_exists: false,
        })
        .await
    }

    async fn alter_table(&self, request: AlterTableRequest) -> LakeFormatResult<()> {
        let identifier = paimon_identifier(request.table.parts())?;
        let mut changes = Vec::with_capacity(request.alterations.len());
        for alteration in request.alterations {
            changes.push(match alteration {
                TableAlteration::AddColumn(field) => {
                    let field = arrow_field_to_paimon(field.as_ref().clone())?;
                    SchemaChange::AddColumn {
                        field_names: vec![field.name().to_string()],
                        data_type: field.data_type().clone(),
                        comment: field.description().map(str::to_string),
                        column_move: None,
                    }
                }
                TableAlteration::DropColumn(name) => SchemaChange::DropColumn {
                    field_names: vec![name],
                },
                TableAlteration::RenameColumn { from, to } => SchemaChange::RenameColumn {
                    field_names: vec![from],
                    new_name: to,
                },
                TableAlteration::SetProperty { key, value } => {
                    SchemaChange::SetOption { key, value }
                }
            });
        }
        self.catalog
            .alter_table(&identifier, changes, false)
            .await
            .map_err(catalog_error)
    }

    async fn drop_table(&self, request: DropTableRequest) -> LakeFormatResult<()> {
        let identifier = paimon_identifier(request.table.parts())?;
        self.catalog
            .drop_table(&identifier, request.if_exists)
            .await
            .map_err(catalog_error)
    }

    async fn rename_table(
        &self,
        from: &TableIdentifier,
        to: &TableIdentifier,
        if_exists: bool,
    ) -> LakeFormatResult<()> {
        let from = paimon_identifier(from.parts())?;
        let to = paimon_identifier(to.parts())?;
        self.catalog
            .rename_table(&from, &to, if_exists)
            .await
            .map_err(catalog_error)
    }
}

#[async_trait]
impl LakeTablePlanner for PaimonFormat {
    async fn plan_read(
        &self,
        session: &dyn Session,
        request: ReadPlanRequest,
    ) -> LakeFormatResult<ReadPlan> {
        let identifier = paimon_identifier(request.table.parts())?;
        let mut table = self
            .catalog
            .get_table(&identifier)
            .await
            .map_err(catalog_error)?;
        let snapshot_version = match request.snapshot {
            Some(snapshot_id) => {
                let snapshot = table
                    .snapshot_manager()
                    .get_snapshot(snapshot_id)
                    .await
                    .map_err(catalog_error)?;
                table = table.copy_with_pinned_snapshot(Some(&snapshot));
                Some(snapshot.id())
            }
            None => table
                .resolve_read_snapshot()
                .await
                .map_err(catalog_error)?
                .map(|snapshot| snapshot.id()),
        };
        let provider = PaimonTableProvider::try_new(table).map_err(planning_error)?;
        let execution = provider
            .scan(
                session,
                request.projection.as_ref(),
                &request.filters,
                request.limit,
            )
            .await?;
        Ok(ReadPlan {
            execution,
            snapshot_version,
            scan_authority: ScanAuthority::Client,
        })
    }

    async fn plan_changes(
        &self,
        _session: &dyn Session,
        request: ChangeReadPlanRequest,
    ) -> LakeFormatResult<ReadPlan> {
        let identifier = paimon_identifier(request.table.parts())?;
        let table = self
            .catalog
            .get_table(&identifier)
            .await
            .map_err(catalog_error)?;
        change_read::plan_changes(table, request.interval).await
    }

    async fn plan_write(
        &self,
        session: &dyn Session,
        request: WritePlanRequest,
    ) -> LakeFormatResult<WritePlan> {
        let identifier = paimon_identifier(request.table.parts())?;
        let table = self
            .catalog
            .get_table(&identifier)
            .await
            .map_err(catalog_error)?;
        if let Some(expected) = request.expected_snapshot {
            let current = table
                .resolve_read_snapshot()
                .await
                .map_err(catalog_error)?
                .map(|snapshot| snapshot.id())
                .unwrap_or(-1);
            if current != expected {
                return Err(LakeFormatError::Commit(format!(
                    "Paimon table `{}` moved from snapshot {expected} to {current}",
                    request.table
                )));
            }
        }
        let input: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(request.input));
        let execution = match request.mode {
            WriteMode::Change => {
                let input = project_change_write(input, &table)?;
                let schema = input.schema();
                // The provider sink expects table columns only. Change writes also carry
                // `_VALUE_KIND`, so they need a sink with the projected input schema.
                Arc::new(DataSinkExec::new(
                    input,
                    Arc::new(PaimonChangeSink::new(table, schema)),
                    None,
                )) as Arc<dyn ExecutionPlan>
            }
            WriteMode::Append | WriteMode::Overwrite => {
                let provider = PaimonTableProvider::try_new(table).map_err(planning_error)?;
                let op = if request.mode == WriteMode::Overwrite {
                    InsertOp::Overwrite
                } else {
                    InsertOp::Append
                };
                provider.insert_into(session, input, op).await?
            }
        };
        Ok(WritePlan {
            execution,
            commit_authority: "paimon-table-commit".to_string(),
        })
    }
}

fn project_change_write(
    input: Arc<dyn ExecutionPlan>,
    table: &Table,
) -> LakeFormatResult<Arc<dyn ExecutionPlan>> {
    let schema = input.schema();
    let table_schema =
        paimon::arrow::build_read_arrow_schema(table.schema().fields()).map_err(planning_error)?;
    let mut projections = table_schema
        .fields()
        .iter()
        .map(|field| {
            let index = schema.index_of(field.name()).map_err(planning_error)?;
            Ok(ProjectionExpr {
                expr: Arc::new(Column::new(field.name(), index)) as Arc<dyn PhysicalExpr>,
                alias: field.name().to_string(),
            })
        })
        .collect::<LakeFormatResult<Vec<_>>>()?;
    let kind_index = schema
        .index_of(IVM_ROW_KIND_COLUMN)
        .map_err(planning_error)?;
    let kind = Arc::new(Column::new(IVM_ROW_KIND_COLUMN, kind_index)) as Arc<dyn PhysicalExpr>;
    let signed =
        |value| Arc::new(Literal::new(ScalarValue::Int32(Some(value)))) as Arc<dyn PhysicalExpr>;
    let native =
        |value| Arc::new(Literal::new(ScalarValue::Int8(Some(value)))) as Arc<dyn PhysicalExpr>;
    let kind = CaseExpr::try_new(
        Some(kind),
        vec![
            (
                signed(RowKind::Positive.code()),
                native(PaimonRowKind::Insert.to_value()),
            ),
            (
                signed(RowKind::Negative.code()),
                native(PaimonRowKind::Delete.to_value()),
            ),
        ],
        Some(native(i8::MAX)),
    )?;
    projections.push(ProjectionExpr {
        expr: Arc::new(kind),
        alias: VALUE_KIND_FIELD_NAME.to_string(),
    });
    Ok(Arc::new(ProjectionExec::try_new(projections, input)?))
}

pub async fn register_paimon_catalog(
    context: &mut SQLContext,
    name: impl Into<String>,
    catalog: Arc<dyn Catalog>,
) -> PipelineResult<()> {
    context
        .register_catalog(name, catalog)
        .await
        .map_err(|error| PipelineError::Catalog(error.to_string()))
}

async fn table_metadata(
    table: Table,
    table_path: TableIdentifier,
) -> LakeFormatResult<Arc<dyn LakeTable>> {
    let schema =
        paimon::arrow::build_read_arrow_schema(table.schema().fields()).map_err(catalog_error)?;
    let properties = table
        .schema()
        .options()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let latest_version = table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .map_err(catalog_error)?;
    Ok(Arc::new(PaimonTable {
        identifier: table_path,
        format: LakeFormatKind::Paimon,
        location: Some(table.location().to_string()),
        latest_version,
        schema,
        properties,
    }))
}

#[derive(Debug)]
struct PaimonTable {
    identifier: TableIdentifier,
    format: LakeFormatKind,
    location: Option<String>,
    latest_version: Option<i64>,
    schema: datafusion::arrow::datatypes::SchemaRef,
    properties: Vec<(String, String)>,
}

impl LakeTable for PaimonTable {
    fn identifier(&self) -> &TableIdentifier {
        &self.identifier
    }

    fn format(&self) -> &LakeFormatKind {
        &self.format
    }

    fn location(&self) -> Option<&str> {
        self.location.as_deref()
    }

    fn latest_version(&self) -> Option<i64> {
        self.latest_version
    }

    fn schema(&self) -> &datafusion::arrow::datatypes::SchemaRef {
        &self.schema
    }

    fn properties(&self) -> &[(String, String)] {
        &self.properties
    }
}

fn arrow_field_to_paimon(
    field: datafusion::arrow::datatypes::Field,
) -> LakeFormatResult<paimon::spec::DataField> {
    paimon::arrow::arrow_fields_to_paimon(&[field])
        .map_err(catalog_error)?
        .into_iter()
        .next()
        .ok_or_else(|| LakeFormatError::InvalidRequest("empty Arrow field".to_string()))
}

fn paimon_identifier(table: &[String]) -> LakeFormatResult<Identifier> {
    match table {
        [database, object] | [_, database, object] => {
            Ok(Identifier::new(database.clone(), object.clone()))
        }
        _ => Err(LakeFormatError::InvalidRequest(format!(
            "Paimon source table must be `database.table` or `catalog.database.table`, got `{}`",
            table.join(".")
        ))),
    }
}

fn catalog_error(error: impl std::fmt::Display) -> LakeFormatError {
    LakeFormatError::Catalog(error.to_string())
}

fn planning_error(error: impl std::fmt::Display) -> LakeFormatError {
    LakeFormatError::Planning(datafusion::error::DataFusionError::Plan(error.to_string()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    use super::*;

    #[test]
    fn managed_options_force_dv_and_l0_compaction() -> LakeFormatResult<()> {
        let options = normalize_managed_table_options(vec![
            (table_options::BUCKET.into(), "1".into()),
            (
                table_options::DELETION_VECTORS_ENABLED.into(),
                "false".into(),
            ),
        ])
        .into_iter()
        .collect::<HashMap<_, _>>();
        for (key, expected) in [
            (table_options::BUCKET, "1"),
            (table_options::DELETION_VECTORS_ENABLED, "true"),
            (table_options::COMMIT_FORCE_COMPACT, "true"),
            (table_options::COMPACTION_FORCE_UP_LEVEL_0, "true"),
            (table_options::WRITE_ONLY, "false"),
            (table_options::WRITE_BUFFER_SPILLABLE, "false"),
            (table_options::LOOKUP_REMOTE_FILE_ENABLED, "true"),
        ] {
            assert_eq!(options.get(key).map(String::as_str), Some(expected));
        }
        Ok(())
    }

    #[test]
    fn accepts_qualified_paimon_identifiers() -> LakeFormatResult<()> {
        let identifier = paimon_identifier(&[
            "lake".to_string(),
            "analytics".to_string(),
            "orders".to_string(),
        ])?;
        assert_eq!(identifier.database(), "analytics");
        assert_eq!(identifier.object(), "orders");
        Ok(())
    }

    #[test]
    fn converts_arrow_field_to_paimon_column() -> LakeFormatResult<()> {
        let field = datafusion::arrow::datatypes::Field::new(
            "event_time",
            datafusion::arrow::datatypes::DataType::Timestamp(
                datafusion::arrow::datatypes::TimeUnit::Microsecond,
                None,
            ),
            false,
        );
        let field = arrow_field_to_paimon(field)?;
        assert_eq!(field.name(), "event_time");
        assert!(!field.data_type().is_nullable());
        Ok(())
    }

    #[tokio::test]
    async fn runs_catalog_ddl_through_format_trait() -> LakeFormatResult<()> {
        let directory = tempfile::tempdir().map_err(catalog_error)?;
        let mut options = Options::new();
        options.set(
            "warehouse",
            directory.path().join("warehouse").to_string_lossy(),
        );
        let format = PaimonFormat::from_options(options)
            .await
            .map_err(catalog_error)?;
        format
            .create_database(CreateDatabaseRequest {
                database: "analytics".to_string(),
                properties: Vec::new(),
                if_not_exists: true,
            })
            .await?;
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Decimal128(18, 2), true),
        ]));
        let metadata = format
            .create_table(CreateTableRequest {
                table: "lake.analytics.orders".parse().expect("valid table"),
                schema,
                partition_by: Vec::new(),
                primary_key: vec!["id".to_string()],
                properties: Vec::new(),
                comment: Some("DynamicLake test table".to_string()),
                ignore_if_exists: false,
            })
            .await?;
        assert_eq!(metadata.format(), &LakeFormatKind::Paimon);
        assert_eq!(metadata.schema().fields().len(), 2);
        assert_eq!(metadata.latest_version(), None);
        Ok(())
    }
}
