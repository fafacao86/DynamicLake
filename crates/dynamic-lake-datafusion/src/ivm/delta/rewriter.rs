use datafusion::logical_expr::LogicalPlan;
use dynamic_lake_core::{PipelineError, PipelineResult};

use crate::ivm::cte::CteQuery;
use crate::ivm::error::IvmError;
use crate::ivm::normalize::{materialized_view_delta_plan, row_id_columns};
use crate::ivm::ojv::OjvAnalyzer;

use super::IvmRefreshContext;
use super::context::IvmDeltaRewriteContext;
use super::handlers::{LinearDeltaHandler, OjvDeltaHandler, UnionDeltaHandler};
use super::scope::IvmDeltaRewriteScope;
use super::snapshot::IvmSnapshotPlanner;

/// Match-based delta rewriter for linear operators, UNION ALL, and OJV joins.
pub struct IvmDeltaRewriter<'a> {
    context: IvmDeltaRewriteContext<'a>,
    linear: LinearDeltaHandler,
    union: UnionDeltaHandler,
    ojv: OjvDeltaHandler,
}

impl<'a> IvmDeltaRewriter<'a> {
    pub(super) fn new(context: IvmDeltaRewriteContext<'a>) -> Self {
        Self {
            context,
            linear: LinearDeltaHandler,
            union: UnionDeltaHandler,
            ojv: OjvDeltaHandler,
        }
    }

    pub fn rewrite(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>, IvmError> {
        if !self.may_change(plan) {
            return Ok(None);
        }
        match plan {
            LogicalPlan::TableScan(scan) => self.linear.rewrite_scan(scan, self),
            LogicalPlan::Projection(projection) => self.linear.rewrite_projection(projection, self),
            LogicalPlan::Filter(filter) => self.linear.rewrite_filter(filter, self),
            LogicalPlan::Union(union) => self.union.rewrite(union, self),
            LogicalPlan::Join(_) => self.ojv.rewrite(plan, self),
            other => Err(IvmError::UnsupportedOperator(format!(
                "IVM delta rewrite does not support: {}",
                other.display()
            ))),
        }
    }

    pub fn may_change(&self, plan: &LogicalPlan) -> bool {
        self.context.may_change(plan)
    }

    pub(super) fn context(&self) -> &IvmDeltaRewriteContext<'a> {
        &self.context
    }
}

/// Top-level IVM rewrite orchestrator.
pub struct IvmRewriter {
    refresh: IvmRefreshContext,
    analysis: crate::ivm::ojv::OjvAnalysis,
}

impl IvmRewriter {
    pub fn new(normalized: &LogicalPlan, refresh: IvmRefreshContext) -> Result<Self, IvmError> {
        let analysis = OjvAnalyzer.analyze(normalized)?;
        Ok(Self { refresh, analysis })
    }

    pub fn rewrite_outer_join_primary(
        &self,
        normalized: &LogicalPlan,
    ) -> Result<Option<LogicalPlan>, IvmError> {
        let scope = IvmDeltaRewriteScope::analyze(normalized, &self.refresh);
        let snapshots = IvmSnapshotPlanner::new(&self.refresh);
        let context = IvmDeltaRewriteContext {
            refresh: &self.refresh,
            analysis: &self.analysis,
            scope: &scope,
            snapshots: &snapshots,
        };
        IvmDeltaRewriter::new(context).rewrite(normalized)
    }
}

/// Build the complete incremental MV query from a normalized definition.
pub fn plan_incremental_view(
    normalized: &LogicalPlan,
    refresh: IvmRefreshContext,
) -> PipelineResult<Option<CteQuery>> {
    let rewriter = IvmRewriter::new(normalized, refresh)
        .map_err(|error| PipelineError::Invalid(error.to_string()))?;
    let Some(delta) = rewriter
        .rewrite_outer_join_primary(normalized)
        .map_err(|error| PipelineError::Invalid(error.to_string()))?
    else {
        return Ok(None);
    };
    let plan = materialized_view_delta_plan(delta, row_id_columns(normalized.schema()).len())
        .map_err(|error| PipelineError::Invalid(error.to_string()))?;
    Ok(Some(CteQuery::new(plan).map_err(|error| {
        PipelineError::Invalid(error.to_string())
    })?))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::prelude::SessionContext;
    use dynamic_lake_core::TableIdentifier;

    use crate::ivm::IncrementInterval;
    use crate::ivm::normalize::{IvmNormalizer, ROW_ID_COLUMN};

    use super::*;

    async fn session() -> Result<SessionContext, Box<dyn std::error::Error>> {
        let session = SessionContext::new();
        for (name, key) in [("a", "a_id"), ("b", "b_id")] {
            session.register_table(
                name,
                Arc::new(EmptyTable::new(Arc::new(Schema::new(vec![
                    Field::new(ROW_ID_COLUMN, DataType::Int64, false),
                    Field::new(key, DataType::Int64, false),
                ])))),
            )?;
        }
        Ok(session)
    }

    fn refresh(changed: &str) -> IvmRefreshContext {
        let changed_table: TableIdentifier = changed.parse().expect("changed table");
        let intervals = ["a", "b"]
            .into_iter()
            .map(|table| {
                let interval = if table == changed {
                    IncrementInterval::try_new(1, 2).expect("interval")
                } else {
                    IncrementInterval::try_new(2, 2).expect("interval")
                };
                (table.parse().expect("source"), interval)
            })
            .collect::<BTreeMap<_, _>>();
        IvmRefreshContext::new(changed_table, intervals).expect("refresh context")
    }

    async fn rewrite(
        sql: &str,
        changed: &str,
    ) -> Result<Option<LogicalPlan>, Box<dyn std::error::Error>> {
        let session = session().await?;
        let plan = session.state().create_logical_plan(sql).await?;
        let normalized = IvmNormalizer::default()
            .normalize(plan, &dynamic_lake_format::LakeFormatRegistry::new())
            .await?;
        Ok(IvmRewriter::new(&normalized.plan, refresh(changed))?
            .rewrite_outer_join_primary(&normalized.plan)?)
    }

    #[tokio::test]
    async fn linear_rewrite_produces_delta_scan() -> Result<(), Box<dyn std::error::Error>> {
        let plan = rewrite("SELECT a_id FROM a WHERE a_id > 0", "a")
            .await?
            .expect("delta plan");
        let display = plan.display_indent().to_string();
        assert!(display.contains("IvmDeltaScan"));
        assert!(
            plan.schema()
                .has_column_with_unqualified_name("__ivm_row_kind")
        );
        Ok(())
    }

    #[tokio::test]
    async fn union_rewrites_only_changed_branch() -> Result<(), Box<dyn std::error::Error>> {
        let plan = rewrite("SELECT a_id FROM a UNION ALL SELECT b_id FROM b", "a")
            .await?
            .expect("delta plan");
        let display = plan.display_indent().to_string();
        assert_eq!(display.matches("IvmDeltaScan").count(), 1);
        assert!(!display.contains("Filter: Boolean(false)"));
        Ok(())
    }

    #[tokio::test]
    async fn left_join_uses_snapshot_and_primary_delta() -> Result<(), Box<dyn std::error::Error>> {
        let plan = rewrite("SELECT a_id, b_id FROM a LEFT JOIN b ON a_id = b_id", "b")
            .await?
            .expect("delta plan");
        let display = plan.display_indent().to_string();
        assert!(display.contains("IvmDeltaScan"));
        assert!(display.contains("IvmSnapshotScan"));
        assert!(display.contains("_ivm_sha1"));
        assert!(display.contains("LeftSemi Join"));
        assert!(display.contains("LeftAnti Join"));
        Ok(())
    }

    #[tokio::test]
    async fn preserved_side_change_uses_primary_only() -> Result<(), Box<dyn std::error::Error>> {
        let plan = rewrite("SELECT a_id, b_id FROM a LEFT JOIN b ON a_id = b_id", "a")
            .await?
            .expect("delta plan");
        let display = plan.display_indent().to_string();
        assert!(!display.contains("LeftSemi Join"));
        assert!(!display.contains("LeftAnti Join"));
        assert!(!display.contains("Boolean(false)"));
        Ok(())
    }
}
