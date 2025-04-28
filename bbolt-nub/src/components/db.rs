use parking_lot::RwLock;
use std::sync;

pub struct DbHandle {}

#[derive(Clone)]
pub struct BoltDb {
  inner: sync::Arc<RwLock<DbHandle>>,
}
