use std::sync::Arc;

use datafusion::logical_expr::logical_plan::{Projection, Union};
use datafusion::logical_expr::{Expr, LogicalPlan};

use crate::ivm::error::IvmError;
use crate::ivm::normalize::ROW_KIND_COLUMN;

use super::super::IvmDeltaRewriter;
use super::column_by_name;

pub(crate) struct UnionDeltaHandler;

impl UnionDeltaHandler {
    pub(crate) fn rewrite(
        &self,
        union: &Union,
        rewriter: &IvmDeltaRewriter<'_>,
    ) -> Result<Option<LogicalPlan>, IvmError> {
        let mut branches = Vec::new();
        for branch in &union.inputs {
            if rewriter.may_change(branch) {
                let delta = rewriter.rewrite(branch)?.ok_or(IvmError::MissingDelta)?;
                branches.push(project_delta_branch(&delta, union)?);
            }
        }
        if branches.is_empty() {
            return Ok(None);
        }
        if branches.len() == 1 {
            return Ok(Some((*branches.pop().expect("one delta branch")).clone()));
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
