use std::collections::BTreeMap;

use dynamic_lake_core::{IncrementInterval, TableIdentifier};

use crate::ivm::error::IvmError;
use crate::ivm::ojv::OjvAnalysis;

use super::IvmSnapshotPlanner;
use super::scope::IvmDeltaRewriteScope;

#[derive(Debug, Clone)]
pub struct IvmRefreshContext {
    changed_table: TableIdentifier,
    intervals: BTreeMap<TableIdentifier, IncrementInterval>,
}

impl IvmRefreshContext {
    pub fn new(
        changed_table: TableIdentifier,
        intervals: BTreeMap<TableIdentifier, IncrementInterval>,
    ) -> Result<Self, IvmError> {
        if !intervals.contains_key(&changed_table) {
            return Err(IvmError::MissingDelta);
        }
        Ok(Self {
            changed_table,
            intervals,
        })
    }

    pub fn changed_table(&self) -> &TableIdentifier {
        &self.changed_table
    }

    pub fn interval_for(&self, table: &TableIdentifier) -> Option<IncrementInterval> {
        self.intervals.get(table).copied()
    }

    pub fn intervals(&self) -> &BTreeMap<TableIdentifier, IncrementInterval> {
        &self.intervals
    }
}

pub(super) struct IvmDeltaRewriteContext<'a> {
    pub(super) refresh: &'a IvmRefreshContext,
    pub(super) analysis: &'a OjvAnalysis,
    pub(super) scope: &'a IvmDeltaRewriteScope,
    pub(super) snapshots: &'a IvmSnapshotPlanner<'a>,
}

impl IvmDeltaRewriteContext<'_> {
    pub(super) fn may_change(&self, plan: &datafusion::logical_expr::LogicalPlan) -> bool {
        self.scope.may_change(plan)
    }
}
