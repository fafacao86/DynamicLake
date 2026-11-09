use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::LogicalPlan;

use crate::ivm::error::IvmError;
use crate::ivm::plan_utils::table_identifier;

use super::context::IvmRefreshContext;
use super::scan::IvmScanNode;

/// Rewrites each table scan in a subtree to its before or after snapshot.
pub struct IvmSnapshotPlanner<'a> {
    refresh: &'a IvmRefreshContext,
}

impl<'a> IvmSnapshotPlanner<'a> {
    pub fn new(refresh: &'a IvmRefreshContext) -> Self {
        Self { refresh }
    }

    pub fn before(&self, plan: &LogicalPlan) -> Result<LogicalPlan, IvmError> {
        self.snapshot(plan, true)
    }

    pub fn after(&self, plan: &LogicalPlan) -> Result<LogicalPlan, IvmError> {
        self.snapshot(plan, false)
    }

    fn snapshot(&self, plan: &LogicalPlan, before: bool) -> Result<LogicalPlan, IvmError> {
        plan.clone()
            .transform_up(|plan| {
                let LogicalPlan::TableScan(scan) = plan else {
                    return Ok(Transformed::no(plan));
                };
                let table = table_identifier(&scan.table_name)?;
                let interval = self.refresh.interval_for(&table).ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "missing IVM interval for {}",
                        table
                    ))
                })?;
                let version = if before {
                    interval.before_exclusive
                } else {
                    interval.after_inclusive
                };
                Ok(Transformed::yes(
                    IvmScanNode::snapshot(scan, version)?.into_plan(),
                ))
            })
            .map(|transformed| transformed.data)
            .map_err(IvmError::DataFusion)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::prelude::SessionContext;
    use dynamic_lake_core::{IncrementInterval, TableIdentifier};

    use super::*;

    #[tokio::test]
    async fn expands_both_snapshot_sides_recursively() -> Result<(), Box<dyn std::error::Error>> {
        let session = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        for name in ["a", "b"] {
            session.register_table(name, Arc::new(EmptyTable::new(Arc::clone(&schema))))?;
        }
        let plan = session
            .state()
            .create_logical_plan("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id")
            .await?;
        let intervals = [("a", 1, 2), ("b", 3, 4)]
            .into_iter()
            .map(|(table, before, after)| {
                Ok((
                    table.parse::<TableIdentifier>()?,
                    IncrementInterval::try_new(before, after)?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, Box<dyn std::error::Error>>>()?;
        let refresh = IvmRefreshContext::new("a".parse()?, intervals)?;
        let snapshots = IvmSnapshotPlanner::new(&refresh);

        let before = snapshots.before(&plan)?.display_indent().to_string();
        assert!(before.contains("IvmSnapshotScan: table=a, version=1"));
        assert!(before.contains("IvmSnapshotScan: table=b, version=3"));

        let after = snapshots.after(&plan)?.display_indent().to_string();
        assert!(after.contains("IvmSnapshotScan: table=a, version=2"));
        assert!(after.contains("IvmSnapshotScan: table=b, version=4"));
        Ok(())
    }
}
