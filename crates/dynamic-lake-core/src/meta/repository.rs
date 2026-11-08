use std::collections::BTreeMap;
use std::sync::Arc;

use crate::NodeId;

use super::key::{
    mv_definition_key, stream_intervals_key, stream_object_key, stream_objects_prefix,
};
use super::{
    IncrementInterval, IvmStreamObject, KvTransaction, MaterializedViewDefinition, MetastoreError,
    MetastoreResult, StreamIntervals, TransactionalKvStore,
};

#[derive(Clone)]
pub struct MetastoreRepository {
    store: Arc<dyn TransactionalKvStore>,
}

impl MetastoreRepository {
    pub fn new(store: Arc<dyn TransactionalKvStore>) -> Self {
        Self { store }
    }

    /// Create the MV definition, stream objects, and initial stream offsets in
    /// one transaction.
    pub async fn create_materialized_view(
        &self,
        definition: &MaterializedViewDefinition,
        streams: &[IvmStreamObject],
    ) -> MetastoreResult<()> {
        let mut transaction = self.store.begin().await?;
        let result = async {
            for stream in streams {
                validate_stream(definition, stream)?;
            }
            let definition_key = mv_definition_key(&definition.node_id)?;
            let definition_value = encode(definition)?;
            transaction.put(definition_key, definition_value).await?;

            let mut intervals = BTreeMap::new();
            for stream in streams {
                let stream_key = stream_object_key(&definition.node_id, &stream.name)?;
                transaction.put(stream_key, encode(stream)?).await?;
                if let Some(version) = stream.consumed_version {
                    intervals.insert(
                        stream.source.clone(),
                        IncrementInterval::try_new(version, version)?,
                    );
                }
            }
            let intervals = StreamIntervals::new(definition.node_id.clone(), intervals);
            transaction
                .put(
                    stream_intervals_key(&definition.node_id)?,
                    encode(&intervals)?,
                )
                .await
        }
        .await;
        finish_or_rollback(transaction, result).await
    }

    pub async fn save_stream_intervals(&self, intervals: &StreamIntervals) -> MetastoreResult<()> {
        let key = stream_intervals_key(&intervals.node_id)?;
        let value = encode(intervals)?;
        let mut transaction = self.store.begin().await?;
        let result = transaction.put(key, value).await;
        finish_or_rollback(transaction, result).await
    }

    pub async fn load_stream_intervals(
        &self,
        node_id: &NodeId,
    ) -> MetastoreResult<Option<StreamIntervals>> {
        self.load(stream_intervals_key(node_id)?).await
    }

    pub async fn save_mv_definition(
        &self,
        definition: &MaterializedViewDefinition,
    ) -> MetastoreResult<()> {
        let key = mv_definition_key(&definition.node_id)?;
        let value = encode(definition)?;
        let mut transaction = self.store.begin().await?;
        let result = transaction.put(key, value).await;
        finish_or_rollback(transaction, result).await
    }

    pub async fn load_mv_definition(
        &self,
        node_id: &NodeId,
    ) -> MetastoreResult<Option<MaterializedViewDefinition>> {
        self.load(mv_definition_key(node_id)?).await
    }

    pub async fn save_stream_object(&self, stream: &IvmStreamObject) -> MetastoreResult<()> {
        let key = stream_object_key(&stream.mv_node_id, &stream.name)?;
        let value = encode(stream)?;
        let mut transaction = self.store.begin().await?;
        let result = transaction.put(key, value).await;
        finish_or_rollback(transaction, result).await
    }

    pub async fn load_stream_object(
        &self,
        node_id: &NodeId,
        stream_name: &str,
    ) -> MetastoreResult<Option<IvmStreamObject>> {
        self.load(stream_object_key(node_id, stream_name)?).await
    }

    pub async fn list_stream_objects(
        &self,
        node_id: &NodeId,
    ) -> MetastoreResult<Vec<IvmStreamObject>> {
        let prefix = stream_objects_prefix(node_id)?;
        let mut transaction = self.store.begin().await?;
        let rows = transaction.scan_prefix(&prefix).await?;
        transaction.rollback().await?;
        rows.into_iter().map(|(_, value)| decode(&value)).collect()
    }

    /// Atomically persist the definition and the source intervals it consumed.
    pub async fn save_refresh_metadata(
        &self,
        definition: &MaterializedViewDefinition,
        intervals: &StreamIntervals,
    ) -> MetastoreResult<()> {
        if definition.node_id != intervals.node_id {
            return Err(MetastoreError::Encoding(
                "MV definition and stream intervals refer to different nodes".to_string(),
            ));
        }
        let definition_key = mv_definition_key(&definition.node_id)?;
        let definition_value = encode(definition)?;
        let intervals_key = stream_intervals_key(&intervals.node_id)?;
        let intervals_value = encode(intervals)?;
        let mut transaction = self.store.begin().await?;
        let result = async {
            transaction.put(definition_key, definition_value).await?;
            transaction.put(intervals_key, intervals_value).await
        }
        .await;
        finish_or_rollback(transaction, result).await
    }

    async fn load<T>(&self, key: Vec<u8>) -> MetastoreResult<Option<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        let mut transaction = self.store.begin().await?;
        let value = transaction.get(&key).await?;
        transaction.rollback().await?;
        value.map(|value| decode(&value)).transpose()
    }
}

fn validate_stream(
    definition: &MaterializedViewDefinition,
    stream: &IvmStreamObject,
) -> MetastoreResult<()> {
    if stream.mv_node_id != definition.node_id {
        return Err(MetastoreError::Encoding(
            "stream object does not belong to the MV definition".to_string(),
        ));
    }
    Ok(())
}

fn encode<T: serde::Serialize>(value: &T) -> MetastoreResult<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| MetastoreError::Encoding(error.to_string()))
}

fn decode<T: serde::de::DeserializeOwned>(value: &[u8]) -> MetastoreResult<T> {
    serde_json::from_slice(value).map_err(|error| MetastoreError::Encoding(error.to_string()))
}

async fn finish_or_rollback(
    transaction: Box<dyn KvTransaction>,
    result: MetastoreResult<()>,
) -> MetastoreResult<()> {
    match result {
        Ok(()) => transaction.commit().await,
        Err(error) => {
            transaction.rollback().await?;
            Err(error)
        }
    }
}
