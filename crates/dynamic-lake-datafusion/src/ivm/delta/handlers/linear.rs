use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::common::Column;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::logical_plan::{Filter, Projection, TableScan};
use datafusion::logical_expr::{Expr, LogicalPlan};
use dynamic_lake_core::TableIdentifier;

use crate::ivm::error::IvmError;
use crate::ivm::normalize::ROW_ID_COLUMN;

use super::super::IvmDeltaRewriter;
use super::super::scan::IvmDeltaScanNode;
use super::super::scope::{is_changed_table, reference_parts};

pub(crate) struct LinearDeltaHandler;

impl LinearDeltaHandler {
    pub(crate) fn rewrite_scan(
        &self,
        scan: &TableScan,
        rewriter: &IvmDeltaRewriter<'_>,
    ) -> Result<Option<LogicalPlan>, IvmError> {
        if !is_changed_table(&scan.table_name, rewriter.context().refresh.changed_table()) {
            return Ok(None);
        }
        let table = table_identifier(&scan.table_name)?;
        let interval = rewriter
            .context()
            .refresh
            .interval_for(&table)
            .ok_or(IvmError::MissingDelta)?;
        if !interval.has_changes() {
            return Ok(None);
        }
        Ok(Some(
            IvmDeltaScanNode::new(scan.clone(), interval)?.into_plan(),
        ))
    }

    pub(crate) fn rewrite_projection(
        &self,
        projection: &Projection,
        rewriter: &IvmDeltaRewriter<'_>,
    ) -> Result<Option<LogicalPlan>, IvmError> {
        let Some(child) = rewriter.rewrite(&projection.input)? else {
            return Ok(None);
        };
        let original_row_id_count = projection
            .schema
            .fields()
            .iter()
            .filter(|field| field.name() == ROW_ID_COLUMN)
            .count();
        let mut exprs = Vec::new();
        for (expr, (_, field)) in projection.expr.iter().zip(projection.schema.iter()) {
            if field.name() == ROW_ID_COLUMN && original_row_id_count != 1 {
                continue;
            }
            exprs.push(remap_columns(expr.clone(), &child)?);
        }
        let mut hidden_names = projection
            .schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<BTreeSet<_>>();
        for (qualifier, field) in child.schema().iter() {
            if field.name().starts_with("__ivm_")
                && !(field.name() == ROW_ID_COLUMN && original_row_id_count == 1)
                && hidden_names.insert(field.name().clone())
            {
                exprs.push(Expr::Column(Column::new(qualifier.cloned(), field.name())));
            }
        }
        Ok(Some(LogicalPlan::Projection(Projection::try_new(
            exprs,
            Arc::new(child),
        )?)))
    }

    pub(crate) fn rewrite_filter(
        &self,
        filter: &Filter,
        rewriter: &IvmDeltaRewriter<'_>,
    ) -> Result<Option<LogicalPlan>, IvmError> {
        let Some(child) = rewriter.rewrite(&filter.input)? else {
            return Ok(None);
        };
        Ok(Some(LogicalPlan::Filter(Filter::try_new(
            filter.predicate.clone(),
            Arc::new(child),
        )?)))
    }
}

fn remap_columns(expr: Expr, child: &LogicalPlan) -> Result<Expr, IvmError> {
    Ok(expr
        .transform_up(|expr| {
            let Expr::Column(column) = expr else {
                return Ok(Transformed::no(expr));
            };
            Ok(Transformed::yes(Expr::Column(resolve_column(
                child, &column,
            )?)))
        })
        .map(|transformed| transformed.data)?)
}

fn resolve_column(plan: &LogicalPlan, column: &Column) -> datafusion::error::Result<Column> {
    if plan.schema().has_column(column) {
        return Ok(column.clone());
    }
    let columns = plan
        .schema()
        .iter()
        .filter(|(_, field)| field.name() == &column.name)
        .map(|(qualifier, field)| Column::new(qualifier.cloned(), field.name()))
        .collect::<Vec<_>>();
    match columns.as_slice() {
        [column] => Ok(column.clone()),
        _ => Err(datafusion::error::DataFusionError::Plan(format!(
            "delta projection cannot resolve column `{column}`"
        ))),
    }
}

fn table_identifier(
    reference: &datafusion::common::TableReference,
) -> datafusion::error::Result<TableIdentifier> {
    TableIdentifier::try_new(reference_parts(reference))
        .map_err(|error| datafusion::error::DataFusionError::Plan(error.to_string()))
}
