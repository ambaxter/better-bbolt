use bbolt_engine::backend::{PagingBackend, PagingSystem};
use std::sync::Arc;

pub mod options;

pub struct Db<T> {
  system: Arc<PagingSystem<T>>,
}

impl<T> Db<T> where T: PagingBackend {}
