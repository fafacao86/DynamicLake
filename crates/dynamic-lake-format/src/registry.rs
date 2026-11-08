use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use dynamic_lake_core::{PipelineCatalog, PipelineError, PipelineResult, TableIdentifier};

use crate::{
    AlterTableRequest, CreateDatabaseRequest, CreateTableRequest, DropDatabaseRequest,
    DropTableRequest, LakeCatalog, LakeFormat, LakeFormatError, LakeFormatResult, LakeTable,
    LakeTablePlanner,
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

#[async_trait]
impl PipelineCatalog for LakeFormatRegistry {
    async fn latest_version(&self, table: &TableIdentifier) -> PipelineResult<i64> {
        let lake_table = self.get_table(table).await.map_err(|error| match error {
            LakeFormatError::TableNotFound(name) => PipelineError::Catalog(name),
            error => PipelineError::Catalog(error.to_string()),
        })?;
        lake_table.latest_version().ok_or_else(|| {
            PipelineError::Catalog(format!(
                "table `{}` does not expose a latest version",
                table
            ))
        })
    }
}
