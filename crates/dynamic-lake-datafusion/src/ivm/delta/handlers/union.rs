use std::sync::Arc;

use datafusion::logical_expr::logical_plan::{Filter, Projection, Union};
use datafusion::logical_expr::{Expr, LogicalPlan, lit};

use crate::ivm::error::IvmError;
use crate::ivm::normalize::{DeltaRowKind, ROW_KIND_COLUMN};

use super::super::IvmDeltaRewriter;
use super::column_by_name;

pub(crate) struct UnionDeltaHandler;

impl UnionDeltaHandler {
    pub(crate) fn rewrite(
        &self,
        union: &Union,
        rewriter: &IvmDeltaRewriter<'_>,
    ) -> Result<Option<LogicalPlan>, IvmError> {
        let mut branches = Vec::with_capacity(union.inputs.len());
        let mut changed = false;
        for branch in &union.inputs {
            if rewriter.may_change(branch) {
                let delta = rewriter.rewrite(branch)?.ok_or(IvmError::MissingDelta)?;
                branches.push(project_delta_branch(&delta, union)?);
                changed = true;
            } else {
                branches.push(empty_delta_branch(branch, union)?);
            }
        }
        if !changed {
            return Ok(None);
        }
        Ok(Some(LogicalPlan::Union(Union::try_new(branches)?)))
    }
}

fn project_delta_branch(plan: &LogicalPlan, union: &Union) -> Result<Arc<LogicalPlan>, IvmError> {
    let mut exprs = plan
        .schema()
        .columns()
        .into_iter()
        .take(union.schema.fields().len())
        .zip(union.schema.fields())
        .map(|(column, field)| Expr::Column(column).alias(field.name()))
        .collect::<Vec<_>>();
    exprs.push(Expr::Column(column_by_name(
        plan.schema(),
        ROW_KIND_COLUMN,
    )?));
    Ok(Arc::new(LogicalPlan::Projection(Projection::try_new(
        exprs,
        Arc::new(plan.clone()),
    )?)))
}

fn empty_delta_branch(
    branch: &Arc<LogicalPlan>,
    union: &Union,
) -> Result<Arc<LogicalPlan>, IvmError> {
    let empty = Arc::new(LogicalPlan::Filter(Filter::try_new(
        lit(false),
        Arc::clone(branch),
    )?));
    let mut exprs = empty
        .schema()
        .columns()
        .into_iter()
        .take(union.schema.fields().len())
        .zip(union.schema.fields())
        .map(|(column, field)| Expr::Column(column).alias(field.name()))
        .collect::<Vec<_>>();
    exprs.push(lit(DeltaRowKind::Positive.code()).alias(ROW_KIND_COLUMN));
    Ok(Arc::new(LogicalPlan::Projection(Projection::try_new(
        exprs, empty,
    )?)))
}
