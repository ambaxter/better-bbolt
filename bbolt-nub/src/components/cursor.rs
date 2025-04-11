use crate::common::errors::CursorError;
use crate::common::id::NodePageId;
use crate::components::bucket::CoreBucket;
use crate::components::tx::TheTx;
use crate::io::pages::types::node::{HasElements, HasValues, NodePage};
use crate::io::pages::{GetKvRefSlice, GetKvTxSlice, TxPageType, TxReadPageIO};
use error_stack::ResultExt;
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
  pub fn element_count(&self) -> usize {
    self.page.element_count()
  }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum CursorLocation {
  Before,
  After,
  Inside,
}

impl CursorLocation {
  fn is_before(self) -> bool {
    matches!(self, CursorLocation::Before)
  }

  fn is_after(self) -> bool {
    matches!(self, CursorLocation::After)
  }

  #[inline]
  fn is_outside(self) -> bool {
    matches!(self, CursorLocation::Before | CursorLocation::After)
  }

  #[inline]
  fn is_inside(self) -> bool {
    matches!(self, CursorLocation::Inside)
  }
}

pub struct CoreCursor<'a, 'tx: 'a, T: TheTx<'tx>> {
  bucket: &'a CoreBucket<'tx, T>,
  stack: Vec<StackEntry<'tx, T::TxPageType>>,
  location: CursorLocation,
}

impl<'p, 'tx, T: TheTx<'tx>> CoreCursor<'p, 'tx, T> {
  pub fn new(bucket: &'p CoreBucket<'tx, T>) -> Self {
    bucket.tx.stats().inc_cursor_count(1);
    Self {
      bucket,
      stack: vec![],
      location: CursorLocation::Before,
    }
  }
  // <T::TxPageType as GetKvRefSlice<'a>::RefKv<'a>
  pub fn key_value_ref<'a>(
    &'a self,
  ) -> Option<(
    <T::TxPageType as GetKvRefSlice>::RefKv<'a>,
    <T::TxPageType as GetKvRefSlice>::RefKv<'a>,
  )> {
    if self.location.is_outside() {
      return None;
    }
    assert!(!self.stack.is_empty());
    let last = self.stack.last().unwrap();
    if last.element_count() == 0 {
      None
    } else {
      match &last.page {
        NodePage::Branch(_) => unreachable!("cannot be branch"),
        NodePage::Leaf(leaf) => leaf.key_value_ref(last.index),
      }
    }
  }

  fn key_value(
    &self,
  ) -> Option<(
    <T::TxPageType as GetKvTxSlice<'tx>>::TxKv,
    <T::TxPageType as GetKvTxSlice<'tx>>::TxKv,
  )> {
    if self.location.is_outside() {
      return None;
    }
    assert!(!self.stack.is_empty());
    let last = self.stack.last().unwrap();
    if last.element_count() == 0 {
      None
    } else {
      match &last.page {
        NodePage::Branch(_) => unreachable!("cannot be branch"),
        NodePage::Leaf(leaf) => leaf.key_value(last.index),
      }
    }
  }

  fn move_to_first_element(&mut self) -> crate::Result<(), CursorError> {
    self.stack.clear();
    self.stack.push(StackEntry::new(self.bucket.root.clone()));

    self.move_to_first_element_on_stack()?;

    if self.stack.last().expect("stack empty").element_count() == 0 {
      self.move_to_next_element()?;
    }

    Ok(())
  }

  fn move_to_first_element_on_stack(&mut self) -> crate::Result<(), CursorError> {
    loop {
      assert_ne!(0, self.stack.len());
      let r = self.stack.last().expect("stack empty");
      if r.is_leaf() {
        break;
      }

      let node_page_id = match &r.page {
        NodePage::Branch(branch) => branch.elements()[r.index].page_id(),
        NodePage::Leaf(_) => unreachable!("Cannot be leaf"),
      };

      let node = self
        .bucket
        .tx
        .read_node_page(node_page_id)
        .change_context(CursorError::GoToFirstElement)?;
      self.stack.push(StackEntry::new(node));
    }

    Ok(())
  }

  fn move_to_prev_element(&mut self) -> crate::Result<(), CursorError> {
    todo!()
  }

  fn move_to_next_element(&mut self) -> crate::Result<(), CursorError> {
    todo!()
  }

  fn move_to_last_element(&mut self) -> crate::Result<(), CursorError> {
    todo!()
  }
}
