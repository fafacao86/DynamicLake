use std::sync::Arc;

use crate::NodeId;

use super::key::{
    mv_definition_key, mv_definitions_prefix, stream_object_key, stream_objects_prefix,
};
use super::{
    IvmStreamObject, KvTransaction, MaterializedViewDefinition, MetastoreError, MetastoreResult,
    TransactionalKvStore,
};

#[derive(Clone)]
pub struct MetastoreRepository {
    store: Arc<dyn TransactionalKvStore>,
}

impl MetastoreRepository {
    pub fn new(store: Arc<dyn TransactionalKvStore>) -> Self {
        Self { store }
    }

    /// Create the MV definition and its stream cursors in one transaction.
    pub async fn create_materialized_view(
        &self,
        definition: &MaterializedViewDefinition,
        streams: &[IvmStreamObject],
    ) -> MetastoreResult<()> {
        let mut transaction = self.store.begin().await?;
        let result = async {
            if definition.plan_signature.is_empty() {
                return Err(MetastoreError::Encoding(
                    "materialized view plan signature must not be empty".to_string(),
                ));
            }
            for stream in streams {
                validate_stream(definition, stream)?;
            }
            let definition_key = mv_definition_key(&definition.node_id)?;
            let definition_value = encode(definition)?;
            transaction.put(definition_key, definition_value).await?;

            for stream in streams {
                let stream_key = stream_object_key(&definition.node_id, &stream.name)?;
                transaction.put(stream_key, encode(stream)?).await?;
            }
            Ok(())
        }
        .await;
        finish_or_rollback(transaction, result).await
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

    pub async fn list_mv_definitions(&self) -> MetastoreResult<Vec<MaterializedViewDefinition>> {
        let mut transaction = self.store.begin().await?;
        let rows = transaction.scan_prefix(&mv_definitions_prefix()?).await?;
        transaction.rollback().await?;
        rows.into_iter().map(|(_, value)| decode(&value)).collect()
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

    /// Persist the definition and advanced source cursors in one metadata transaction.
    /// Call this only after the corresponding view output has committed.
    pub async fn save_refresh_metadata(
        &self,
        definition: &MaterializedViewDefinition,
        streams: &[IvmStreamObject],
    ) -> MetastoreResult<()> {
        for stream in streams {
            validate_stream(definition, stream)?;
        }
        let definition_key = mv_definition_key(&definition.node_id)?;
        let definition_value = encode(definition)?;
        let mut transaction = self.store.begin().await?;
        let result = async {
            let stored = transaction.get(&definition_key).await?.ok_or_else(|| {
                MetastoreError::Encoding("materialized view definition does not exist".to_string())
            })?;
            let previous: MaterializedViewDefinition = decode(&stored)?;
            if previous.plan_signature.is_empty()
                || previous.plan_signature != definition.plan_signature
            {
                return Err(MetastoreError::Encoding(format!(
                    "IVM plan signature changed for `{}`; full refresh required",
                    definition.node_id
                )));
            }
            transaction.put(definition_key, definition_value).await?;
            for stream in streams {
                let key = stream_object_key(&definition.node_id, &stream.name)?;
                let existing = transaction.get(&key).await?.ok_or_else(|| {
                    MetastoreError::Encoding(format!("stream `{}` does not exist", stream.name))
                })?;
                let previous: IvmStreamObject = decode(&existing)?;
                if previous.source != stream.source || previous.last_version > stream.last_version {
                    return Err(MetastoreError::Encoding(format!(
                        "stream `{}` has an invalid cursor advance",
                        stream.name
                    )));
                }
                transaction.put(key, encode(stream)?).await?;
            }
            Ok(())
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
