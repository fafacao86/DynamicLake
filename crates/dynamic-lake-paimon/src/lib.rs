//! Apache Paimon implementation of the DynamicLake lake-format SPI.
//!
//! DAG and refresh code never imports Paimon types. The format registry talks
//! to this crate through `LakeCatalog` and `LakeTablePlanner`.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use dynamic_lake_core::{PipelineCatalog, PipelineError, PipelineResult, TableIdentifier};
use dynamic_lake_format::{
    AlterTableRequest, CreateDatabaseRequest, CreateTableRequest, DropDatabaseRequest,
    DropTableRequest, FormatCapability, LakeCatalog, LakeFormat, LakeFormatError, LakeFormatKind,
    LakeFormatResult, LakeTable, LakeTablePlanner, ReadPlan, ReadPlanRequest, ScanAuthority,
    TableAlteration, WriteMode, WritePlan, WritePlanRequest,
};
use paimon::catalog::{Catalog, Identifier};
use paimon::spec::{Schema, SchemaChange};
use paimon::{Options, Table};
use paimon_datafusion::{PaimonTableProvider, SQLContext};

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
        let provider = PaimonTableProvider::try_new(table).map_err(planning_error)?;
        let insert_op = match request.mode {
            WriteMode::Append => InsertOp::Append,
            WriteMode::Overwrite => InsertOp::Overwrite,
        };
        let input = Arc::new(CoalescePartitionsExec::new(request.input));
        let execution = provider.insert_into(session, input, insert_op).await?;
        Ok(WritePlan {
            execution,
            commit_authority: "paimon-table-commit".to_string(),
        })
    }
}

#[derive(Clone)]
pub struct PaimonPipelineCatalog {
    format: Arc<PaimonFormat>,
}

impl PaimonPipelineCatalog {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self {
            format: Arc::new(PaimonFormat::new(catalog)),
        }
    }

    pub fn from_format(format: Arc<PaimonFormat>) -> Self {
        Self { format }
    }
}

#[async_trait]
impl PipelineCatalog for PaimonPipelineCatalog {
    async fn latest_version(&self, table: &TableIdentifier) -> PipelineResult<i64> {
        self.format
            .metadata(table)
            .await
            .map_err(|error| PipelineError::Catalog(error.to_string()))?
            .latest_version()
            .ok_or_else(|| {
                PipelineError::Catalog(format!(
                    "Paimon table `{}` does not expose a latest version",
                    table
                ))
            })
    }
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
