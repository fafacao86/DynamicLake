use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::common::Column;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::logical_plan::{Filter, Projection, TableScan};
use datafusion::logical_expr::{Expr, LogicalPlan};

use crate::ivm::error::IvmError;
use crate::ivm::normalize::ROW_ID_COLUMN;
use crate::ivm::plan_utils::{resolve_column, table_identifier};

use super::super::IvmDeltaRewriter;
use super::super::scan::IvmScanNode;
use super::super::scope::is_changed_table;

pub(crate) struct LinearDeltaHandler;

impl LinearDeltaHandler {
    pub(crate) fn rewrite_scan(
        &self,
        scan: &TableScan,
        rewriter: &IvmDeltaRewriter<'_>,
    ) -> Result<Option<LogicalPlan>, IvmError> {
        let refresh = rewriter.context().refresh;
        if !is_changed_table(&scan.table_name, refresh.changed_table()) {
            return Ok(None);
        }
        let table = table_identifier(&scan.table_name)?;
        let interval = refresh.interval_for(&table).ok_or(IvmError::MissingDelta)?;
        if !interval.has_changes() {
            return Ok(None);
        }
        Ok(Some(
            IvmScanNode::delta(scan.clone(), interval)?.into_plan(),
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
            Ok(Transformed::yes(Expr::Column(
                resolve_column(child, &column).ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "delta projection cannot resolve column `{column}`"
                    ))
                })?,
            )))
        })
        .map(|transformed| transformed.data)?)
}
