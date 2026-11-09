use std::sync::Arc;

use datafusion::common::{Column, DFSchema, TableReference};
use datafusion::logical_expr::logical_plan::Projection;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan};
use dynamic_lake_core::TableIdentifier;

pub(super) fn project_plan(
    input: LogicalPlan,
    exprs: Vec<Expr>,
) -> datafusion::error::Result<LogicalPlan> {
    let fields = exprs
        .iter()
        .map(|expr| expr.to_field(input.schema()))
        .collect::<datafusion::error::Result<Vec<_>>>()?;
    let schema = Arc::new(DFSchema::new_with_metadata(
        fields,
        input.schema().metadata().clone(),
    )?);
    Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
        exprs,
        Arc::new(input),
        schema,
    )?))
}

pub(super) fn reference_parts(reference: &TableReference) -> Vec<String> {
    let mut parts = Vec::new();
    if let Some(catalog) = reference.catalog() {
        parts.push(catalog.to_owned());
    }
    if let Some(schema) = reference.schema() {
        parts.push(schema.to_owned());
    }
    parts.push(reference.table().to_owned());
    parts
}

pub(super) fn table_identifier(
    reference: &TableReference,
) -> datafusion::error::Result<TableIdentifier> {
    TableIdentifier::try_new(reference_parts(reference))
        .map_err(|error| datafusion::error::DataFusionError::Plan(error.to_string()))
}

pub(super) fn resolve_column(plan: &LogicalPlan, source: &Column) -> Option<Column> {
    plan.schema().has_column(source).then(|| source.clone())
}
