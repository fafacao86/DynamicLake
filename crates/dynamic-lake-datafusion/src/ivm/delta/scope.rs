use std::collections::HashMap;

use datafusion::common::TableReference;
use datafusion::logical_expr::LogicalPlan;

use super::IvmRefreshContext;

/// Per-refresh cache of subtrees that contain the changed base table.
pub struct IvmDeltaRewriteScope {
    changed: HashMap<LogicalPlan, bool>,
}

impl IvmDeltaRewriteScope {
    pub fn analyze(root: &LogicalPlan, refresh: &IvmRefreshContext) -> Self {
        let mut scope = Self {
            changed: HashMap::new(),
        };
        scope.visit(root, refresh.changed_table());
        scope
    }

    pub fn may_change(&self, plan: &LogicalPlan) -> bool {
        self.changed.get(plan).copied().unwrap_or(false)
    }

    fn visit(
        &mut self,
        plan: &LogicalPlan,
        changed_table: &dynamic_lake_core::TableIdentifier,
    ) -> bool {
        if let Some(changed) = self.changed.get(plan) {
            return *changed;
        }
        let mut changed = match plan {
            LogicalPlan::TableScan(scan) => is_changed_table(&scan.table_name, changed_table),
            _ => false,
        };
        if !changed {
            changed = plan
                .inputs()
                .into_iter()
                .any(|input| self.visit(input, changed_table));
        }
        self.changed.insert(plan.clone(), changed);
        changed
    }
}

pub(super) fn is_changed_table(
    reference: &TableReference,
    changed_table: &dynamic_lake_core::TableIdentifier,
) -> bool {
    let reference_parts = reference_parts(reference);
    let changed_parts = changed_table.parts();
    if reference_parts.len() > changed_parts.len() {
        return false;
    }
    let changed_suffix = &changed_parts[changed_parts.len() - reference_parts.len()..];
    reference_parts
        .iter()
        .zip(changed_suffix)
        .all(|(reference, changed)| reference.eq_ignore_ascii_case(changed))
}

pub(super) fn reference_parts(reference: &TableReference) -> Vec<String> {
    match reference {
        TableReference::Bare { table } => vec![table.to_string()],
        TableReference::Partial { schema, table } => {
            vec![schema.to_string(), table.to_string()]
        }
        TableReference::Full {
            catalog,
            schema,
            table,
        } => vec![catalog.to_string(), schema.to_string(), table.to_string()],
    }
}
