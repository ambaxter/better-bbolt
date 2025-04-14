use crate::common::errors::CursorError;
use crate::common::id::NodePageId;
use crate::common::layout::node::LeafFlag;
use crate::components::bucket::CoreBucket;
use crate::components::tx::TheTx;
use crate::io::pages::lazy::ops::TryPartialOrd;
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
  Begin,
  End,
  Inside,
}

impl CursorLocation {
  fn is_begin(self) -> bool {
    matches!(self, CursorLocation::Begin)
  }

  fn is_end(self) -> bool {
    matches!(self, CursorLocation::End)
  }

  #[inline]
  fn is_outside(self) -> bool {
    matches!(self, CursorLocation::Begin | CursorLocation::End)
  }

  #[inline]
  fn is_inside(self) -> bool {
    matches!(self, CursorLocation::Inside)
  }
}

pub struct CoreCursor<'p, 'tx: 'p, T: TheTx<'tx>> {
  bucket: &'p CoreBucket<'tx, T>,
  stack: Vec<StackEntry<'tx, T::TxPageType>>,
  location: CursorLocation,
}

impl<'p, 'tx, T: TheTx<'tx>> CoreCursor<'p, 'tx, T> {
  pub fn new(bucket: &'p CoreBucket<'tx, T>) -> Self {
    bucket.tx.stats().inc_cursor_count(1);
    Self {
      bucket,
      stack: vec![],
      location: CursorLocation::Begin,
    }
  }

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
    if last.element_count() == 0 || last.index > last.page.element_count() {
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
    if last.element_count() == 0 || last.index > last.page.element_count() {
      None
    } else {
      match &last.page {
        NodePage::Branch(_) => unreachable!("cannot be branch"),
        NodePage::Leaf(leaf) => leaf.key_value(last.index),
      }
    }
  }

  fn move_to_first_element(&mut self) -> crate::Result<Option<LeafFlag>, CursorError> {
    self.stack.clear();
    self.stack.push(StackEntry::new(self.bucket.root.clone()));

    self.move_to_first_element_on_stack()?;

    if self.stack.last().expect("stack empty").element_count() == 0 {
      self.move_to_next_element()?;
    }

    if self.location.is_inside() {
      let last = self.stack.last().expect("stack empty");
      match &last.page {
        NodePage::Branch(_) => unreachable!("cannot be branch"),
        NodePage::Leaf(leaf) => Ok(leaf.leaf_flag(last.index)),
      }
    } else {
      Ok(None)
    }
  }

  fn move_to_first_element_on_stack(&mut self) -> crate::Result<(), CursorError> {
    assert!(!self.stack.is_empty());
    loop {
      let entry = self.stack.last().expect("stack empty");
      if entry.is_leaf() {
        break;
      }

      let node_page_id = match &entry.page {
        NodePage::Branch(branch) => branch.elements()[entry.index].page_id(),
        NodePage::Leaf(_) => unreachable!("Cannot be leaf"),
      };

      let node = self
        .bucket
        .tx
        .read_node_page(node_page_id)
        .change_context(CursorError::GoToFirstElement)?;
      self.stack.push(StackEntry::new(node));
    }
    self.location = CursorLocation::Inside;
    Ok(())
  }

  fn move_to_next_element(&mut self) -> crate::Result<Option<LeafFlag>, CursorError> {
    loop {
      // Attempt to move over one element until we're successful.
      // Move up the stack as we hit the end of each page in our stack.
      let mut stack_exhausted = true;
      let mut new_stack_depth = 0;
      for (depth, entry) in self.stack.iter_mut().enumerate().rev() {
        new_stack_depth = depth + 1;
        if entry.index < entry.element_count() {
          entry.index += 1;
          stack_exhausted = false;
          break;
        }
      }

      // If we've hit the root page then stop and return. This will leave the
      // cursor on the last element of the last page.
      if stack_exhausted {
        self.location = CursorLocation::End;
        return Ok(None);
      }

      // Otherwise start from where we left off in the stack and find the
      // first element of the first leaf page.
      self.stack.truncate(new_stack_depth);
      self.move_to_first_element_on_stack()?;

      // If this is an empty page then restart and move back up the stack.
      // https://github.com/boltdb/bolt/issues/450
      let entry = self.stack.last_mut().expect("stack empty");
      if entry.element_count() == 0 {
        continue;
      }
      match &entry.page {
        NodePage::Branch(_) => unreachable!("cannot be branch"),
        NodePage::Leaf(leaf) => return Ok(leaf.leaf_flag(entry.index)),
      }
    }
  }

  fn move_to_prev_element(&mut self) -> crate::Result<Option<LeafFlag>, CursorError> {
    // Attempt to move back one element until we're successful.
    // Move up the stack as we hit the beginning of each page in our stack.
    let mut new_stack_depth = 0;
    let mut stack_exhausted = true;
    for (depth, entry) in self.stack.iter_mut().enumerate().rev() {
      new_stack_depth = depth + 1;
      if entry.index > 0 {
        entry.index -= 1;
        stack_exhausted = false;
        break;
      }
      // If we've hit the beginning, we should stop moving the cursor,
      // and stay at the first element, so that users can continue to
      // iterate over the elements in reverse direction by calling `Next`.
      // We should return nil in such case.
      // Refer to https://github.com/etcd-io/bbolt/issues/733
      if new_stack_depth == 1 {
        self.move_to_first_element_on_stack()?;
        self.location = CursorLocation::Begin;
        return Ok(None);
      }
    }
    if stack_exhausted {
      self.stack.truncate(0);
    } else {
      self.stack.truncate(new_stack_depth);
    }

    // If we've hit the end then return None
    if self.stack.is_empty() {
      self.location = CursorLocation::Begin;
      return Ok(None);
    }

    // Move down the stack to find the last element of the last leaf under this branch.
    self.move_to_last_element_on_stack()?;

    let entry = self.stack.last_mut().expect("stack empty");
    match &entry.page {
      NodePage::Branch(_) => unreachable!("cannot be branch"),
      NodePage::Leaf(leaf) => Ok(leaf.leaf_flag(entry.index)),
    }
  }

  fn move_to_last_element_on_stack(&mut self) -> crate::Result<(), CursorError> {
    assert!(!self.stack.is_empty());
    loop {
      // Exit when we hit a leaf page.
      let entry = self.stack.last_mut().expect("stack empty");
      if entry.is_leaf() {
        break;
      }
      let node_page_id = match &entry.page {
        NodePage::Branch(branch) => branch.elements()[entry.index].page_id(),
        NodePage::Leaf(_) => unreachable!("Cannot be leaf"),
      };

      let node = self
        .bucket
        .tx
        .read_node_page(node_page_id)
        .change_context(CursorError::GoToLastElement)?;
      let element_index = node.element_count().saturating_sub(1);
      self
        .stack
        .push(StackEntry::new_with_index(node, element_index));
    }
    self.location = CursorLocation::Inside;
    Ok(())
  }

  fn seek<'a>(&'a mut self, v: &[u8]) -> crate::Result<Option<LeafFlag>, CursorError>
  where for<'b>
    <T::TxPageType as GetKvRefSlice>::RefKv<'b>: PartialOrd<[u8]>,
  {
    self.stack.clear();
    self.stack.push(StackEntry::new(self.bucket.root.clone()));
    self.seek_branches(v)?;
    Ok(self.seek_leaf(v))
  }

  fn seek_branches<'a>(&'a mut self, v: &[u8]) -> crate::Result<(), CursorError>
  where for<'b>
    <T::TxPageType as GetKvRefSlice>::RefKv<'b>: PartialOrd<[u8]>,
  {
    assert!(!self.stack.is_empty());
    loop {
      let node_page_id = {
        // Exit when we hit a leaf page.
        let entry = self.stack.last_mut().expect("stack empty");
        if entry.is_leaf() {
          break;
        }
        let branch = match &entry.page {
          NodePage::Branch(branch) => branch,
          NodePage::Leaf(_) => unreachable!("Cannot be leaf"),
        };
        let node_index = branch.search_branch(v);
        entry.index = node_index;
        branch.elements()[node_index].page_id()
      };

      let node = self
        .bucket
        .tx
        .read_node_page(node_page_id)
        .change_context(CursorError::Seek)?;
      self.stack.push(StackEntry::new(node));
    }
    Ok(())
  }

  fn seek_leaf<'a>(&'a mut self, v: &[u8]) -> Option<LeafFlag>
  where for<'b>
    <T::TxPageType as GetKvRefSlice>::RefKv<'b>: PartialOrd<[u8]>,
  {
    assert!(!self.stack.is_empty());
    let entry = self.stack.last_mut().expect("stack empty");
    assert!(entry.is_leaf());
    let leaf = match &entry.page {
      NodePage::Branch(_) => unreachable!("cannot be branch"),
      NodePage::Leaf(leaf) => leaf,
    };
    match leaf.search_leaf(v) {
      Ok(exact) => {
        entry.index = exact;
        leaf.leaf_flag(entry.index)
      }
      Err(closest) => {
        entry.index = closest;
        None
      }
    }
  }

  fn try_seek<'a>(&'a mut self, v: &[u8]) -> crate::Result<Option<LeafFlag>, CursorError>
  where for<'b>
    <T::TxPageType as GetKvRefSlice>::RefKv<'b>: TryPartialOrd<[u8]>,
  {
    self.stack.clear();
    self.stack.push(StackEntry::new(self.bucket.root.clone()));
    self.try_seek_branches(v)?;
    self.try_seek_leaf(v)
  }

  fn try_seek_branches<'a>(&'a mut self, v: &[u8]) -> crate::Result<(), CursorError>
  where for<'b>
    <T::TxPageType as GetKvRefSlice>::RefKv<'b>: TryPartialOrd<[u8]>
  {
    assert!(!self.stack.is_empty());
    /*loop {
      let node_page_id = {
        // Exit when we hit a leaf page.
        let entry = self.stack.last_mut().expect("stack empty");
        if entry.is_leaf() {
          break;
        }
        let branch = match &entry.page {
          NodePage::Branch(branch) => branch,
          NodePage::Leaf(_) => unreachable!("Cannot be leaf"),
        };
        let node_index = branch
          .try_search_branch(v)
          .change_context(CursorError::Seek)?;
        let node_index = 0;
        entry.index = node_index;
        branch.elements()[node_index].page_id()
      };

      let node = self
        .bucket
        .tx
        .read_node_page(node_page_id)
        .change_context(CursorError::Seek)?;
      self.stack.push(StackEntry::new(node));
    }*/
    Ok(())
  }

  fn try_seek_leaf<'a>(&'a mut self, v: &[u8]) -> crate::Result<Option<LeafFlag>, CursorError>
  where
    <T::TxPageType as GetKvRefSlice>::RefKv<'a>: TryPartialOrd<[u8]>,
  {
    todo!()
  }
}
