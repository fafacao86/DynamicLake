use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use dynamic_lake_core::{KvTransaction, MetastoreError, MetastoreResult, TransactionalKvStore};
use redb::Database;

use super::transaction::RedbTransaction;

pub struct RedbMetastore {
    db: Arc<Database>,
}

impl RedbMetastore {
    pub fn open(path: impl AsRef<Path>) -> MetastoreResult<Self> {
        let db = Database::create(path).map_err(backend_error)?;
        Ok(Self { db: Arc::new(db) })
    }
}

#[async_trait]
impl TransactionalKvStore for RedbMetastore {
    async fn begin(&self) -> MetastoreResult<Box<dyn KvTransaction>> {
        let transaction = self.db.begin_write().map_err(backend_error)?;
        Ok(Box::new(RedbTransaction::new(transaction)))
    }
}

pub(super) fn backend_error(error: impl std::fmt::Display) -> MetastoreError {
    MetastoreError::Backend(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use dynamic_lake_core::{
        IvmStreamObject, MaterializedViewDefinition, MetastoreRepository, NodeId, TableName,
    };

    use super::*;

    #[tokio::test]
    async fn create_mv_persists_stream_objects_atomically() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(
            RedbMetastore::open(directory.path().join("metadata.redb")).expect("open redb"),
        );
        let repository = MetastoreRepository::new(store);
        let node_id: NodeId = "paimon.default.answer".parse().expect("node id");
        let source: TableName = "paimon.default.orders".parse().expect("source");
        let definition = MaterializedViewDefinition {
            node_id: node_id.clone(),
            definition_format: "datafusion-logical-plan-json".to_string(),
            definition: b"normalized-plan".to_vec(),
            plan_signature: "sig".to_string(),
            output_version: Some(3),
        };
        let stream = IvmStreamObject::create(node_id.clone(), source, Some(2));

        repository
            .create_materialized_view(&definition, std::slice::from_ref(&stream))
            .await
            .expect("create MV metadata");

        assert_eq!(
            repository
                .load_mv_definition(&node_id)
                .await
                .expect("load definition"),
            Some(definition.clone())
        );
        assert_eq!(
            repository
                .list_stream_objects(&node_id)
                .await
                .expect("list streams"),
            vec![stream.clone()]
        );
        let advanced = stream.advanced_to(4).expect("advance cursor");
        let mut incompatible = definition.clone();
        incompatible.plan_signature = "different-layout".to_string();
        assert!(
            repository
                .save_refresh_metadata(&incompatible, std::slice::from_ref(&advanced))
                .await
                .is_err()
        );
        assert_eq!(
            repository
                .list_stream_objects(&node_id)
                .await
                .expect("load unchanged stream"),
            vec![stream.clone()]
        );
        repository
            .save_refresh_metadata(&definition, std::slice::from_ref(&advanced))
            .await
            .expect("save refresh metadata");
        assert_eq!(
            repository
                .list_stream_objects(&node_id)
                .await
                .expect("list advanced streams"),
            vec![advanced]
        );
    }
}
