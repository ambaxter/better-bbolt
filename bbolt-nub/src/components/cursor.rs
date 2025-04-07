use crate::common::id::NodePageId;
use crate::components::bucket::CoreBucket;
use crate::components::tx::TheTx;
use crate::io::pages::{TxPageType, TxReadPageIO};
use crate::io::pages::types::node::NodePage;
pub struct StackEntry<'tx, T> {
  page: NodePage<'tx, T>,
  index: usize,
}

impl<'tx, T> StackEntry<'tx, T> {
  #[inline]
  pub fn new(page: NodePage<'tx, T>) -> Self {
    Self { page, index: 0 }
  }

  #[inline]
  pub fn new_with_index(page: NodePage<'tx, T>, index: usize) -> Self {
    Self { page, index }
  }

  #[inline]
  pub fn is_leaf(&self) -> bool {
    self.page.is_leaf()
  }

  #[inline]
  pub fn is_branch(&self) -> bool {
    self.page.is_branch()
  }
}

impl<'tx, T: 'tx> StackEntry<'tx, T>
where
  T: TxPageType<'tx>,
{
  #[inline]
  pub fn len(&self) -> usize {
    self.page.len()
  }
}

pub struct CoreCursor<'a, 'tx: 'a, T: TheTx<'tx>> {
  bucket: &'a CoreBucket<'tx, T>,
  stack: Vec<StackEntry<'tx, T::TxPageType>>,
}
