use parking_lot::RwLock;
use triomphe::Arc;

pub struct DbHandle {}

#[derive(Clone)]
pub struct BoltDb {
  inner: Arc<RwLock<DbHandle>>,
}
