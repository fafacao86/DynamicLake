use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::TableReference;
use datafusion::datasource::MemTable;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use dynamic_lake_core::TableIdentifier;

use crate::{
    AlterTableRequest, CreateDatabaseRequest, CreateTableRequest, DropDatabaseRequest,
    DropTableRequest, LakeCatalog, LakeFormat, LakeFormatError, LakeFormatResult, LakeTable,
    LakeTablePlanner, ReadPlanRequest, WriteMode, WritePlanRequest,
};

/// Immutable format registry used by planner and execution services.
#[derive(Default, Clone)]
pub struct LakeFormatRegistry {
    formats: BTreeMap<String, Arc<dyn LakeFormat>>,
}

impl LakeFormatRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        mut self,
        catalog_name: impl Into<String>,
        format: Arc<dyn LakeFormat>,
    ) -> Self {
        self.formats
            .insert(catalog_name.into().to_ascii_lowercase(), format);
        self
    }

    pub fn format(&self, catalog_name: &str) -> Option<&Arc<dyn LakeFormat>> {
        self.formats.get(&catalog_name.to_ascii_lowercase())
    }

    pub fn catalog(&self, catalog_name: &str) -> Option<&dyn LakeCatalog> {
        self.format(catalog_name).map(|format| format.catalog())
    }

    pub fn planner(&self, catalog_name: &str) -> Option<&dyn LakeTablePlanner> {
        self.format(catalog_name).map(|format| format.planner())
    }

    pub fn formats(&self) -> &BTreeMap<String, Arc<dyn LakeFormat>> {
        &self.formats
    }

    pub async fn get_table(&self, table: &TableIdentifier) -> LakeFormatResult<Arc<dyn LakeTable>> {
        let format = self.resolve_table(table)?;
        format.catalog().get_table(table).await
    }

    pub async fn create_database(
        &self,
        catalog_name: &str,
        request: CreateDatabaseRequest,
    ) -> LakeFormatResult<()> {
        let format = self.format(catalog_name).ok_or_else(|| {
            LakeFormatError::Catalog(format!(
                "no lake format registered for catalog `{catalog_name}`"
            ))
        })?;
        format.catalog().create_database(request).await
    }

    pub async fn drop_database(
        &self,
        catalog_name: &str,
        request: DropDatabaseRequest,
    ) -> LakeFormatResult<()> {
        let format = self.format(catalog_name).ok_or_else(|| {
            LakeFormatError::Catalog(format!(
                "no lake format registered for catalog `{catalog_name}`"
            ))
        })?;
        format.catalog().drop_database(request).await
    }

    pub async fn create_table(
        &self,
        request: CreateTableRequest,
    ) -> LakeFormatResult<Arc<dyn LakeTable>> {
        let format = self.resolve_table(&request.table)?;
        format.catalog().create_table(request).await
    }

    pub async fn append_batches(
        &self,
        session: &SessionContext,
        table: &TableIdentifier,
        batches: Vec<RecordBatch>,
    ) -> LakeFormatResult<()> {
        self.write_batches(session, table, batches, WriteMode::Append, None)
            .await
    }

    pub async fn overwrite_batches(
        &self,
        session: &SessionContext,
        table: &TableIdentifier,
        batches: Vec<RecordBatch>,
    ) -> LakeFormatResult<()> {
        self.write_batches(session, table, batches, WriteMode::Overwrite, None)
            .await
    }

    pub async fn write_changes(
        &self,
        session: &SessionContext,
        table: &TableIdentifier,
        batches: Vec<RecordBatch>,
        expected_snapshot: i64,
    ) -> LakeFormatResult<()> {
        self.write_batches(
            session,
            table,
            batches,
            WriteMode::Change,
            Some(expected_snapshot),
        )
        .await
    }

    async fn write_batches(
        &self,
        session: &SessionContext,
        table: &TableIdentifier,
        batches: Vec<RecordBatch>,
        mode: WriteMode,
        expected_snapshot: Option<i64>,
    ) -> LakeFormatResult<()> {
        let format = self.resolve_table(table)?;
        let expected_snapshot = match expected_snapshot {
            Some(version) => Some(version),
            None => format
                .catalog()
                .get_table(table)
                .await?
                .latest_version()
                .or(Some(-1)),
        };
        let input = session
            .read_batches(batches)?
            .create_physical_plan()
            .await?;
        let write = format
            .planner()
            .plan_write(
                &session.state(),
                WritePlanRequest {
                    table: table.clone(),
                    input,
                    mode,
                    expected_snapshot,
                },
            )
            .await?;
        collect(write.execution, session.task_ctx()).await?;
        Ok(())
    }

    pub async fn register_table_snapshot(
        &self,
        session: &SessionContext,
        table: &TableIdentifier,
    ) -> LakeFormatResult<()> {
        let [catalog, database, name] = table.parts() else {
            return Err(LakeFormatError::InvalidRequest(format!(
                "table `{table}` must have catalog, database and table names"
            )));
        };
        let format = self.resolve_table(table)?;
        let native_schema = format.catalog().get_table(table).await?.schema().clone();
        let schema = Arc::new(Schema::new(
            native_schema
                .fields()
                .iter()
                .map(|field| {
                    if field.name() == "__ivm_row_id" {
                        Arc::new(field.as_ref().clone().with_nullable(false))
                    } else {
                        Arc::clone(field)
                    }
                })
                .collect::<Vec<_>>(),
        ));
        let read = format
            .planner()
            .plan_read(
                &session.state(),
                ReadPlanRequest {
                    table: table.clone(),
                    snapshot: None,
                    projection: None,
                    filters: Vec::new(),
                    limit: None,
                },
            )
            .await?;
        let batches = collect(read.execution, session.task_ctx())
            .await?
            .into_iter()
            .map(|batch| {
                let columns = batch
                    .columns()
                    .iter()
                    .zip(schema.fields())
                    .map(|(column, field)| {
                        if column.data_type() == field.data_type() {
                            Ok(Arc::clone(column))
                        } else {
                            datafusion::arrow::compute::cast(column, field.data_type())
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                RecordBatch::try_new(schema.clone(), columns)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(datafusion::error::DataFusionError::from)?;
        let provider = Arc::new(MemTable::try_new(schema, vec![batches])?);
        let reference = TableReference::full(catalog.clone(), database.clone(), name.clone());
        let _ = session.deregister_table(reference.clone());
        session.register_table(reference, provider)?;
        Ok(())
    }

    pub async fn alter_table(&self, request: AlterTableRequest) -> LakeFormatResult<()> {
        let format = self.resolve_table(&request.table)?;
        format.catalog().alter_table(request).await
    }

    pub async fn drop_table(&self, request: DropTableRequest) -> LakeFormatResult<()> {
        let format = self.resolve_table(&request.table)?;
        format.catalog().drop_table(request).await
    }

    pub async fn rename_table(
        &self,
        from: &TableIdentifier,
        to: &TableIdentifier,
        if_exists: bool,
    ) -> LakeFormatResult<()> {
        let format = self.resolve_table(from)?;
        format.catalog().rename_table(from, to, if_exists).await
    }

    fn resolve_table(&self, table: &TableIdentifier) -> LakeFormatResult<&Arc<dyn LakeFormat>> {
        let catalog_name = table.parts().first().ok_or_else(|| {
            LakeFormatError::InvalidRequest("table name must not be empty".to_string())
        })?;
        let format = self.format(catalog_name).ok_or_else(|| {
            LakeFormatError::Catalog(format!(
                "no lake format registered for catalog `{catalog_name}`"
            ))
        })?;
        Ok(format)
    }
}
