use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

use datafusion::logical_expr::LogicalPlan;
use serde::{Deserialize, Serialize};

use crate::meta::IvmStreamObject;

/// Fully qualified table name used both as a graph node ID and a source name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TableIdentifier {
    parts: Vec<String>,
}

impl TableIdentifier {
    pub fn try_new(parts: impl IntoIterator<Item = String>) -> crate::PipelineResult<Self> {
        let parts = parts
            .into_iter()
            .map(|part| normalize_identifier(&part))
            .collect::<Vec<_>>();
        if parts.is_empty() || parts.iter().any(String::is_empty) {
            return Err(crate::PipelineError::Invalid(
                "table identifier must not be empty".to_string(),
            ));
        }
        Ok(Self { parts })
    }

    pub fn parts(&self) -> &[String] {
        &self.parts
    }

    pub fn table(&self) -> &str {
        self.parts.last().map_or("", String::as_str)
    }

    pub fn database(&self) -> Option<&str> {
        self.parts
            .len()
            .checked_sub(2)
            .and_then(|index| self.parts.get(index))
            .map(String::as_str)
    }

    fn from_canonical(parts: Vec<String>) -> Self {
        Self { parts }
    }
}

impl fmt::Display for TableIdentifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.parts.join("."))
    }
}

impl FromStr for TableIdentifier {
    type Err = crate::PipelineError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_new(value.split('.').map(ToString::to_string))
    }
}

pub type NodeId = TableIdentifier;
pub type TableName = TableIdentifier;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PipelineBuildOptions {
    pub default_catalog: String,
    pub default_database: Vec<String>,
}

impl Default for PipelineBuildOptions {
    fn default() -> Self {
        Self {
            default_catalog: "paimon".to_string(),
            default_database: vec!["default".to_string()],
        }
    }
}

impl PipelineBuildOptions {
    pub fn canonical_table(&self, parts: &[String]) -> TableIdentifier {
        let mut parts = parts
            .iter()
            .map(|part| normalize_identifier(part))
            .collect::<Vec<_>>();
        if parts.len() == 1 {
            parts.insert(0, normalize_identifier(&self.default_catalog));
            for (offset, database) in self.default_database.iter().enumerate() {
                parts.insert(offset + 1, normalize_identifier(database));
            }
        } else if parts.len() == 2 {
            parts.insert(0, normalize_identifier(&self.default_catalog));
        }
        TableIdentifier::from_canonical(parts)
    }
}

#[derive(Debug, Clone)]
pub struct DynamicTable {
    pub id: NodeId,
    pub query: LogicalPlan,
    pub upstream: BTreeSet<NodeId>,
    pub external_inputs: BTreeSet<TableName>,
    pub stream_objects: BTreeMap<TableName, IvmStreamObject>,
}

impl DynamicTable {
    pub fn new(id: NodeId, query: LogicalPlan) -> Self {
        Self {
            id,
            query,
            upstream: BTreeSet::new(),
            external_inputs: BTreeSet::new(),
            stream_objects: BTreeMap::new(),
        }
    }

    pub fn with_upstream(mut self, id: NodeId) -> Self {
        self.upstream.insert(id);
        self
    }

    pub fn with_external_input(mut self, table: TableName) -> Self {
        self.external_inputs.insert(table);
        self
    }

    pub fn with_stream_object(mut self, stream: IvmStreamObject) -> Self {
        self.stream_objects.insert(stream.source.clone(), stream);
        self
    }
}

pub(crate) fn normalize_identifier(value: &str) -> String {
    value
        .trim()
        .trim_matches('"')
        .trim_matches('`')
        .to_ascii_lowercase()
}
