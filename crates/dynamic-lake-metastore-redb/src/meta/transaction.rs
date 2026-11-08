use async_trait::async_trait;
use dynamic_lake_core::{KvTransaction, MetastoreError, MetastoreResult};
use redb::{ReadableTable, TableDefinition, WriteTransaction};

use super::store::backend_error;

const METADATA: TableDefinition<&[u8], &[u8]> = TableDefinition::new("metadata");

pub(super) struct RedbTransaction {
    transaction: Option<WriteTransaction>,
}

impl RedbTransaction {
    pub(super) fn new(transaction: WriteTransaction) -> Self {
        Self {
            transaction: Some(transaction),
        }
    }

    fn transaction(&self) -> MetastoreResult<&WriteTransaction> {
        self.transaction
            .as_ref()
            .ok_or(MetastoreError::TransactionFinished)
    }
}

#[async_trait]
impl KvTransaction for RedbTransaction {
    async fn get(&mut self, key: &[u8]) -> MetastoreResult<Option<Vec<u8>>> {
        let table = self
            .transaction()?
            .open_table(METADATA)
            .map_err(backend_error)?;
        Ok(table
            .get(key)
            .map_err(backend_error)?
            .map(|value| value.value().to_vec()))
    }

    async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> MetastoreResult<()> {
        let transaction = self.transaction()?;
        let mut table = transaction.open_table(METADATA).map_err(backend_error)?;
        table
            .insert(key.as_slice(), value.as_slice())
            .map_err(backend_error)?;
        Ok(())
    }

    async fn delete(&mut self, key: Vec<u8>) -> MetastoreResult<()> {
        let transaction = self.transaction()?;
        let mut table = transaction.open_table(METADATA).map_err(backend_error)?;
        table.remove(key.as_slice()).map_err(backend_error)?;
        Ok(())
    }

    async fn scan_prefix(&mut self, prefix: &[u8]) -> MetastoreResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let table = self
            .transaction()?
            .open_table(METADATA)
            .map_err(backend_error)?;
        let range = table.range(prefix..).map_err(backend_error)?;
        let mut rows = Vec::new();
        for item in range {
            let (key, value) = item.map_err(backend_error)?;
            let key = key.value();
            if !key.starts_with(prefix) {
                break;
            }
            rows.push((key.to_vec(), value.value().to_vec()));
        }
        Ok(rows)
    }

    async fn commit(mut self: Box<Self>) -> MetastoreResult<()> {
        let transaction = self
            .transaction
            .take()
            .ok_or(MetastoreError::TransactionFinished)?;
        transaction.commit().map_err(backend_error)
    }

    async fn rollback(mut self: Box<Self>) -> MetastoreResult<()> {
        let transaction = self
            .transaction
            .take()
            .ok_or(MetastoreError::TransactionFinished)?;
        transaction.abort().map_err(backend_error)
    }
}

#[cfg(test)]
mod tests {
    use dynamic_lake_core::TransactionalKvStore;

    use super::super::RedbMetastore;

    #[tokio::test]
    async fn reads_own_writes_and_rolls_back() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = RedbMetastore::open(directory.path().join("metadata.redb")).expect("open redb");

        let mut transaction = store.begin().await.expect("transaction");
        transaction
            .put(b"k".to_vec(), b"v".to_vec())
            .await
            .expect("put");
        assert_eq!(
            transaction.get(b"k").await.expect("read"),
            Some(b"v".to_vec())
        );
        transaction.rollback().await.expect("rollback");

        let mut check = store.begin().await.expect("check transaction");
        assert_eq!(check.get(b"k").await.expect("read"), None);
        check.rollback().await.expect("rollback check");
    }

    #[tokio::test]
    async fn scan_prefix_returns_ordered_rows() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = RedbMetastore::open(directory.path().join("metadata.redb")).expect("open redb");
        let mut transaction = store.begin().await.expect("transaction");
        transaction
            .put(b"p/b".to_vec(), b"2".to_vec())
            .await
            .expect("put");
        transaction
            .put(b"p/a".to_vec(), b"1".to_vec())
            .await
            .expect("put");
        assert_eq!(
            transaction.scan_prefix(b"p/").await.expect("scan"),
            vec![
                (b"p/a".to_vec(), b"1".to_vec()),
                (b"p/b".to_vec(), b"2".to_vec())
            ]
        );
        transaction.rollback().await.expect("rollback");
    }
}
