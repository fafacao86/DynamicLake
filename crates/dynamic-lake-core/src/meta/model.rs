use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{NodeId, TableName};

use super::{MetastoreError, MetastoreResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct IncrementInterval {
    pub before_exclusive: i64,
    pub after_inclusive: i64,
}

impl IncrementInterval {
    pub fn try_new(before_exclusive: i64, after_inclusive: i64) -> MetastoreResult<Self> {
        if before_exclusive > after_inclusive {
            return Err(MetastoreError::Encoding(format!(
                "invalid increment interval [{before_exclusive}, {after_inclusive}]"
            )));
        }
        Ok(Self {
            before_exclusive,
            after_inclusive,
        })
    }

    pub fn has_changes(self) -> bool {
        self.before_exclusive != self.after_inclusive
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamIntervals {
    pub node_id: NodeId,
    #[serde(with = "table_interval_map")]
    pub intervals: BTreeMap<TableName, IncrementInterval>,
}

impl StreamIntervals {
    pub fn new(node_id: NodeId, intervals: BTreeMap<TableName, IncrementInterval>) -> Self {
        Self { node_id, intervals }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum StreamStatus {
    #[default]
    Active,
    Stale,
    Disabled,
}

/// Persistent stream created for one MV/base-table pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IvmStreamObject {
    pub name: String,
    pub mv_node_id: NodeId,
    pub source: TableName,
    pub initial_version: Option<i64>,
    pub consumed_version: Option<i64>,
    pub status: StreamStatus,
}

impl IvmStreamObject {
    pub fn create(mv_node_id: NodeId, source: TableName, initial_version: Option<i64>) -> Self {
        Self {
            name: default_stream_name(&mv_node_id, &source),
            mv_node_id,
            source,
            initial_version,
            consumed_version: initial_version,
            status: StreamStatus::Active,
        }
    }
}

/// Opaque serialized MV definition owned by the execution backend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializedViewDefinition {
    pub node_id: NodeId,
    pub definition_format: String,
    pub definition: Vec<u8>,
    pub plan_signature: String,
    pub output_version: Option<i64>,
}

fn default_stream_name(node_id: &NodeId, source: &TableName) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in format!("{node_id}\0{source}").bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("__dynamic_lake_ivm_stream_{hash:016x}")
}

mod table_interval_map {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::TableName;

    use super::IncrementInterval;

    pub fn serialize<S>(
        intervals: &BTreeMap<TableName, IncrementInterval>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        intervals.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<BTreeMap<TableName, IncrementInterval>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Vec::<(TableName, IncrementInterval)>::deserialize(deserializer)
            .map(|intervals| intervals.into_iter().collect())
    }
}
