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
    #[serde(alias = "consumed_version")]
    pub last_version: Option<i64>,
    pub status: StreamStatus,
}

impl IvmStreamObject {
    pub fn create(mv_node_id: NodeId, source: TableName, last_version: Option<i64>) -> Self {
        Self {
            name: default_stream_name(&mv_node_id, &source),
            mv_node_id,
            source,
            last_version,
            status: StreamStatus::Active,
        }
    }

    /// Plan against a fixed source snapshot without advancing the durable cursor.
    pub fn pending_interval(
        &self,
        latest_version: i64,
    ) -> MetastoreResult<Option<IncrementInterval>> {
        let consumed = self.last_version.ok_or_else(|| {
            MetastoreError::Encoding(format!(
                "stream `{}` has no consumed version; initialize it with a full refresh",
                self.name
            ))
        })?;
        let interval = IncrementInterval::try_new(consumed, latest_version)?;
        Ok(interval.has_changes().then_some(interval))
    }

    /// Return an updated cursor for persistence after the view output commits.
    pub fn advanced_to(&self, version: i64) -> MetastoreResult<Self> {
        self.pending_interval(version)?;
        let mut advanced = self.clone();
        advanced.last_version = Some(version);
        Ok(advanced)
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

#[cfg(test)]
mod tests {
    use super::IvmStreamObject;

    #[test]
    fn stream_interval_uses_consumed_version_without_advancing_it() {
        let stream = IvmStreamObject::create(
            "paimon.analytics.mv".parse().unwrap(),
            "paimon.analytics.source".parse().unwrap(),
            Some(3),
        );
        assert_eq!(stream.pending_interval(3).unwrap(), None);
        let interval = stream.pending_interval(5).unwrap().unwrap();
        assert_eq!(
            (interval.before_exclusive, interval.after_inclusive),
            (3, 5)
        );
        assert_eq!(stream.last_version, Some(3));
        assert_eq!(stream.advanced_to(5).unwrap().last_version, Some(5));
        assert!(stream.pending_interval(2).is_err());
    }
}
