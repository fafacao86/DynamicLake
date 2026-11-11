use async_trait::async_trait;
use std::path::Path;
use std::sync::Arc;

use super::MetastoreResult;

/// Transactional byte-oriented key/value metadata store.
///
/// Transactions provide read-your-writes and detect conflicting committed
/// changes at commit time. Dropping an uncommitted transaction rolls it back.
#[async_trait]
pub trait TransactionalKvStore: Send + Sync {
    async fn begin(&self) -> MetastoreResult<Box<dyn KvTransaction>>;
}

/// Opens a metadata store at a server-selected path.
#[async_trait]
pub trait MetastoreFactory: Send + Sync {
    async fn open(&self, path: &Path) -> MetastoreResult<Arc<dyn TransactionalKvStore>>;
}

#[async_trait]
pub trait KvTransaction: Send {
    async fn get(&mut self, key: &[u8]) -> MetastoreResult<Option<Vec<u8>>>;

    async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> MetastoreResult<()>;

    async fn delete(&mut self, key: Vec<u8>) -> MetastoreResult<()>;

    async fn scan_prefix(&mut self, prefix: &[u8]) -> MetastoreResult<Vec<(Vec<u8>, Vec<u8>)>>;

    async fn commit(self: Box<Self>) -> MetastoreResult<()>;

    async fn rollback(self: Box<Self>) -> MetastoreResult<()>;
}
