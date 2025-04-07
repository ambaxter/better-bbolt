use crate::common::id::NodePageId;
use crate::components::bucket::CoreBucket;
use crate::components::tx::TheTx;
use crate::io::pages::types::node::{HasElements, NodePage};
use crate::io::pages::{TxPageType, TxReadPageIO};
use std::process::Output;
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

impl<'a, 'tx, T: TheTx<'tx>> CoreCursor<'a, 'tx, T> {
  pub fn new(bucket: &'a CoreBucket<'tx, T>) -> Self {
    bucket.tx.stats().inc_cursor_count(1);
    Self {
      bucket,
      stack: vec![],
    }
  }

  fn first(
    &mut self,
  ) -> Option<(
    <<T as TxReadPageIO<'tx>>::TxPageType as TxPageType<'tx>>::TxPageBytes,
    <<T as TxReadPageIO<'tx>>::TxPageType as TxPageType<'tx>>::TxPageBytes,
  )> {
    self.stack.clear();
    self.stack.push(StackEntry::new(self.bucket.root.clone()));

    None
  }

  fn go_to_first_element(&mut self) {
    loop {
      let r = self.stack.last().expect("stack empty");
      if r.is_leaf() {
        break;
      }

      let node_page_id = match &r.page {
        NodePage::Branch(branch) => branch.elements().get(r.index).expect("bad index").page_id(),
        NodePage::Leaf(_) => unreachable!("Cannot be leaf"),
      };

      let node = self.bucket.tx.read_node_page(node_page_id);
    }
  }
}
