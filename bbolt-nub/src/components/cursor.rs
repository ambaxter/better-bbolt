use crate::common::data_pool::SharedData;
use crate::common::errors::CursorError;
use crate::common::layout::node::LeafFlag;
use crate::common::vec_pool::UniqueVec;
use crate::components::bucket::OnDiskBucket;
use crate::components::tx::{TheLazyTx, TheTx};
use crate::io::TxSlot;
use crate::io::bytes::ref_bytes::RefTxBytes;
use crate::io::pages::direct::DirectPage;
use crate::io::pages::direct::ops::KvDataType;
use crate::io::pages::lazy::LazyPage;
use crate::io::pages::lazy::ops::{KvTryDataType, TryPartialOrd};
use crate::io::pages::types::node::branch::bbolt::BBoltBranch;
use crate::io::pages::types::node::branch::{HasBranches, HasNodes, HasSearchBranch};
use crate::io::pages::types::node::leaf::bbolt::BBoltLeaf;
use crate::io::pages::types::node::leaf::{HasLeaves, HasSearchLeaf, HasValues};
use crate::io::pages::types::node::{HasElements, HasKeyRefs, HasKeys, NodePage};
use crate::io::pages::{
  GatKvRef, GetGatKvRefSlice, GetKvTxSlice, Page, TxPageType, TxReadLazyPageIO, TxReadPageIO,
};
use error_stack::ResultExt;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync;

#[derive(Clone)]
pub struct StackEntry<B, L> {
  page: NodePage<B, L>,
  index: usize,
}

impl<B, L> StackEntry<B, L> {
  #[inline]
  pub fn new(page: NodePage<B, L>) -> Self {
    Self { page, index: 0 }
  }

  #[inline]
  pub fn new_with_index(page: NodePage<B, L>, index: usize) -> Self {
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

impl<B, L> StackEntry<B, L>
where
  B: Page,
  L: Page,
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

pub trait CoreCursorMoveApi {
  fn move_to_first_element(&mut self) -> crate::Result<Option<LeafFlag>, CursorError>;
  fn move_to_next_element(&mut self) -> crate::Result<Option<LeafFlag>, CursorError>;
  fn move_to_prev_element(&mut self) -> crate::Result<Option<LeafFlag>, CursorError>;
  fn move_to_last_element(&mut self) -> crate::Result<Option<LeafFlag>, CursorError>;
}

pub trait CoreCursorMoveLeafApi<L>: CoreCursorMoveApi {
  fn move_to_prev_leaf(&mut self) -> crate::Result<Option<LeafFlag>, CursorError>;
  fn move_to_next_leaf(&mut self) -> crate::Result<Option<LeafFlag>, CursorError>;
  fn get_leaf_page(&self) -> Option<&L>;
}

pub trait CoreCursorSeekApi {
  fn seek(&mut self, v: &[u8]) -> crate::Result<Option<LeafFlag>, CursorError>;
}

pub trait CoreCursorTrySeekApi {
  fn try_seek(&mut self, v: &[u8]) -> crate::Result<Option<LeafFlag>, CursorError>;
}

pub trait CoreCursorRefApi: CoreCursorMoveApi {
  type KvRef<'a>: GetGatKvRefSlice
  where
    Self: 'a;

  fn key_ref<'a>(&'a self) -> Option<Self::KvRef<'a>>;

  fn value_ref<'a>(&'a self) -> Option<Self::KvRef<'a>>;

  fn key_value_ref<'a>(&'a self) -> Option<(Self::KvRef<'a>, Self::KvRef<'a>)>;
}

pub trait CoreCursorApi<'tx>: CoreCursorMoveApi {
  type KvTx: GetKvTxSlice<'tx>;

  fn key(&self) -> Option<Self::KvTx>;

  fn value(&self) -> Option<Self::KvTx>;

  fn key_value(&self) -> Option<(Self::KvTx, Self::KvTx)>;
}

pub struct CoreCursor<'tx, B, L, TX> {
  tx: sync::Arc<TX>,
  root: NodePage<B, L>,
  stack: UniqueVec<StackEntry<B, L>>,
  location: CursorLocation,
  tx_slot: TxSlot<'tx>,
}

impl<'tx, B, L, TX> Clone for CoreCursor<'tx, B, L, TX> where B: Clone, L: Clone, {
  fn clone(&self) -> Self {
    CoreCursor {
      tx: self.tx.clone(),
      root: self.root.clone(),
      stack: self.stack.clone(),
      location: self.location,
      tx_slot: self.tx_slot,
    }
  }
}

impl<'tx, TX> CoreCursor<'tx, TX::BranchType, TX::LeafType, TX>
where
  TX: TheTx<'tx>,
{
  pub fn new_with_stack(
    bucket: &OnDiskBucket<TX::BranchType, TX::LeafType, TX>,
    stack: UniqueVec<StackEntry<TX::BranchType, TX::LeafType>>,
  ) -> Self {
    bucket.tx.stats().inc_cursor_count(1);
    Self {
      tx: bucket.tx.clone(),
      root: bucket.root.clone(),
      stack,
      location: CursorLocation::Begin,
      tx_slot: TxSlot::default(),
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
        NodePage::Branch(branch) => branch.node(entry.index).unwrap(),
        NodePage::Leaf(_) => unreachable!("Cannot be leaf"),
      };

      let node = self
        .tx
        .read_node_page(node_page_id)
        .change_context(CursorError::GoToFirstElement)?;
      self.stack.push(StackEntry::new(node));
    }
    self.location = CursorLocation::Inside;
    Ok(())
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
        NodePage::Branch(branch) => branch.node(entry.index).unwrap(),
        NodePage::Leaf(_) => unreachable!("Cannot be leaf"),
      };

      let node = self
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

  fn seek_branches<'a>(&'a mut self, v: &[u8]) -> crate::Result<(), CursorError>
  where
    for<'b> <TX::BranchType as GatKvRef<'b>>::KvRef: PartialOrd<[u8]>,
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
        branch.node(entry.index).unwrap()
      };

      let node = self
        .tx
        .read_node_page(node_page_id)
        .change_context(CursorError::Seek)?;
      self.stack.push(StackEntry::new(node));
    }
    Ok(())
  }

  fn seek_leaf<'a>(&'a mut self, v: &[u8]) -> Option<LeafFlag>
  where
    for<'b> <TX::LeafType as GatKvRef<'b>>::KvRef: PartialOrd<[u8]>,
  {
    assert!(!self.stack.is_empty());
    let entry = self.stack.last_mut().expect("stack empty");
    assert!(entry.is_leaf());
    let leaf = match &entry.page {
      NodePage::Branch(_) => unreachable!("cannot be branch"),
      NodePage::Leaf(leaf) => leaf,
    };
    self.location = CursorLocation::Inside;
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

  fn try_seek_branches<'a>(&'a mut self, v: &[u8]) -> crate::Result<(), CursorError>
  where
    for<'b> <TX::BranchType as GatKvRef<'b>>::KvRef: TryPartialOrd<[u8]>,
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
        let node_index = branch
          .try_search_branch(v)
          .change_context(CursorError::Seek)?;
        let node_index = 0;
        entry.index = node_index;
        branch.node(entry.index).unwrap()
      };

      let node = self
        .tx
        .read_node_page(node_page_id)
        .change_context(CursorError::Seek)?;
      self.stack.push(StackEntry::new(node));
    }
    Ok(())
  }

  fn try_seek_leaf<'a>(&'a mut self, v: &[u8]) -> crate::Result<Option<LeafFlag>, CursorError>
  where
    for<'b> <TX::LeafType as GatKvRef<'b>>::KvRef: TryPartialOrd<[u8]>,
  {
    assert!(!self.stack.is_empty());
    let entry = self.stack.last_mut().expect("stack empty");
    assert!(entry.is_leaf());
    let leaf = match &entry.page {
      NodePage::Branch(_) => unreachable!("cannot be branch"),
      NodePage::Leaf(leaf) => leaf,
    };
    self.location = CursorLocation::Inside;
    match leaf.try_search_leaf(v).change_context(CursorError::Seek)? {
      Ok(exact) => {
        entry.index = exact;
        Ok(leaf.leaf_flag(entry.index))
      }
      Err(closest) => {
        entry.index = closest;
        Ok(None)
      }
    }
  }

  fn get_leaf_for_kv(&self) -> Option<(usize, &TX::LeafType)> {
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
        NodePage::Leaf(leaf) => Some((last.index, leaf)),
      }
    }
  }
}

impl<'tx, TX: TheTx<'tx>> CoreCursorMoveApi for CoreCursor<'tx, TX::BranchType, TX::LeafType, TX> {
  fn move_to_first_element(&mut self) -> crate::Result<Option<LeafFlag>, CursorError> {
    self.stack.clear();
    self.stack.push(StackEntry::new(self.root.clone()));

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

  fn move_to_next_element(&mut self) -> crate::Result<Option<LeafFlag>, CursorError> {
    loop {
      // Attempt to move over one element until we're successful.
      // Move up the stack as we hit the end of each page in our stack.
      let mut stack_exhausted = true;
      let mut new_stack_depth = 0;
      for (depth, entry) in self.stack.iter_mut().enumerate().rev() {
        new_stack_depth = depth + 1;
        if entry.index + 1 < entry.element_count() {
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
        NodePage::Leaf(leaf) => return Ok(Some(leaf.leaf_flag(entry.index).unwrap())),
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

  fn move_to_last_element(&mut self) -> crate::Result<Option<LeafFlag>, CursorError> {
    self.stack.clear();
    self.stack.push(StackEntry::new(self.root.clone()));

    self.move_to_last_element_on_stack()?;
    if self.stack.last().expect("stack empty").element_count() == 0 {
      self.move_to_prev_element()?;
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
}

impl<'tx, TX: TheTx<'tx>> CoreCursorMoveLeafApi<TX::LeafType>
  for CoreCursor<'tx, TX::BranchType, TX::LeafType, TX>
{
  fn move_to_prev_leaf(&mut self) -> crate::Result<Option<LeafFlag>, CursorError> {
    if let Some(last) = self.stack.last_mut() {
      last.index = 0;
    }
    self.move_to_prev_element()
  }

  fn move_to_next_leaf(&mut self) -> error_stack::Result<Option<LeafFlag>, CursorError> {
    if let Some(last) = self.stack.last_mut() {
      let index = match &last.page {
        NodePage::Branch(_) => unreachable!("cannot be branch"),
        NodePage::Leaf(leaf) => leaf.element_count() - 1,
      };
      last.index = index;
    }
    self.move_to_next_element()
  }

  fn get_leaf_page(&self) -> Option<&TX::LeafType> {
    self.get_leaf_for_kv().map(|(_, page)| page)
  }
}

impl<'tx, TX: TheTx<'tx>> CoreCursorRefApi for CoreCursor<'tx, TX::BranchType, TX::LeafType, TX> {
  type KvRef<'a>
    = <TX::LeafType as GatKvRef<'a>>::KvRef
  where
    Self: 'a;

  fn key_ref<'a>(&'a self) -> Option<Self::KvRef<'a>> {
    self
      .get_leaf_for_kv()
      .map(|(index, leaf)| leaf.key_ref(index))
      .flatten()
  }

  fn value_ref<'a>(&'a self) -> Option<Self::KvRef<'a>> {
    self
      .get_leaf_for_kv()
      .map(|(index, leaf)| leaf.value_ref(index))
      .flatten()
  }

  fn key_value_ref<'a>(&'a self) -> Option<(Self::KvRef<'a>, Self::KvRef<'a>)> {
    self
      .get_leaf_for_kv()
      .map(|(index, leaf)| leaf.key_value_ref(index))
      .flatten()
  }
}

impl<'tx, TX: TheTx<'tx>> CoreCursorApi<'tx> for CoreCursor<'tx, TX::BranchType, TX::LeafType, TX> {
  type KvTx = <TX::LeafType as HasKeys<'tx>>::TxKv;

  fn key(&self) -> Option<Self::KvTx> {
    self
      .get_leaf_for_kv()
      .map(|(index, leaf)| leaf.key(index))
      .flatten()
  }

  fn value(&self) -> Option<Self::KvTx> {
    self
      .get_leaf_for_kv()
      .map(|(index, leaf)| leaf.value(index))
      .flatten()
  }

  fn key_value(&self) -> Option<(Self::KvTx, Self::KvTx)> {
    self
      .get_leaf_for_kv()
      .map(|(index, leaf)| leaf.key_value(index))
      .flatten()
  }
}

impl<'tx, TX: TheTx<'tx>> CoreCursorSeekApi for CoreCursor<'tx, TX::BranchType, TX::LeafType, TX>
where
  for<'b> <TX::BranchType as GatKvRef<'b>>::KvRef: PartialOrd<[u8]>,
  for<'b> <TX::LeafType as GatKvRef<'b>>::KvRef: PartialOrd<[u8]>,
{
  fn seek(&mut self, v: &[u8]) -> crate::Result<Option<LeafFlag>, CursorError> {
    self.stack.clear();
    self.stack.push(StackEntry::new(self.root.clone()));
    self.seek_branches(v)?;
    Ok(self.seek_leaf(v))
  }
}

impl<'tx, TX: TheTx<'tx>> CoreCursorTrySeekApi for CoreCursor<'tx, TX::BranchType, TX::LeafType, TX>
where
  for<'b> <TX::BranchType as GatKvRef<'b>>::KvRef: TryPartialOrd<[u8]>,
  for<'b> <TX::LeafType as GatKvRef<'b>>::KvRef: TryPartialOrd<[u8]>,
{
  fn try_seek(&mut self, v: &[u8]) -> crate::Result<Option<LeafFlag>, CursorError> {
    self.stack.clear();
    self.stack.push(StackEntry::new(self.root.clone()));
    self.try_seek_branches(v)?;
    self.try_seek_leaf(v)
  }
}

// TODO: LeafFlagFilterCursor is generic over C because I was trying to be lazy
// Calling Bucket.get(&self) which creates a Cursor with &'a Bucket and Cursor.seek(&mut self) fails
// due to Subtyping & Veriance (https://doc.rust-lang.org/nomicon/subtyping.html)
// "mutable references are invariant over their type parameter"
// Ideally we don't want to have to clone 2 Arcs for every Bucket.get() invocation
#[derive(Clone)]
pub struct LeafFlagFilterCursor<C> {
  cursor: C,
  leaf_flag: LeafFlag,
}

impl<C> LeafFlagFilterCursor<C> {
  pub fn new(cursor: C, leaf_flag: LeafFlag) -> Self {
    Self { cursor, leaf_flag }
  }
}

impl<C> CoreCursorMoveApi for LeafFlagFilterCursor<C>
where
  C: CoreCursorMoveApi,
{
  fn move_to_first_element(&mut self) -> crate::Result<Option<LeafFlag>, CursorError> {
    if let Some(flag) = self.cursor.move_to_first_element()? {
      if flag == self.leaf_flag {
        Ok(Some(flag))
      } else {
        self.move_to_next_element()
      }
    } else {
      Ok(None)
    }
  }

  fn move_to_next_element(&mut self) -> crate::Result<Option<LeafFlag>, CursorError> {
    while let Some(flag) = self.cursor.move_to_next_element()? {
      if flag == self.leaf_flag {
        return Ok(Some(flag));
      }
    }
    Ok(None)
  }

  fn move_to_prev_element(&mut self) -> crate::Result<Option<LeafFlag>, CursorError> {
    while let Some(flag) = self.cursor.move_to_prev_element()? {
      if flag == self.leaf_flag {
        return Ok(Some(flag));
      }
    }
    Ok(None)
  }

  fn move_to_last_element(&mut self) -> crate::Result<Option<LeafFlag>, CursorError> {
    if let Some(flag) = self.cursor.move_to_last_element()? {
      if flag == self.leaf_flag {
        Ok(Some(flag))
      } else {
        self.move_to_prev_element()
      }
    } else {
      Ok(None)
    }
  }
}

impl<C> CoreCursorSeekApi for LeafFlagFilterCursor<C>
where
  C: CoreCursorSeekApi,
{
  fn seek(&mut self, v: &[u8]) -> crate::Result<Option<LeafFlag>, CursorError> {
    match self.cursor.seek(v)? {
      None => Ok(None),
      Some(flag) => {
        if flag == self.leaf_flag {
          Ok(Some(flag))
        } else {
          if self.leaf_flag == LeafFlag::BUCKET {
            Err(CursorError::ValueIsABucket.into())
          } else {
            Err(CursorError::ValueIsBytes.into())
          }
        }
      }
    }
  }
}

impl<C> CoreCursorTrySeekApi for LeafFlagFilterCursor<C>
where
  C: CoreCursorTrySeekApi,
{
  fn try_seek(&mut self, v: &[u8]) -> crate::Result<Option<LeafFlag>, CursorError> {
    match self.cursor.try_seek(v)? {
      None => Ok(None),
      Some(flag) => {
        if flag == self.leaf_flag {
          Ok(Some(flag))
        } else {
          if self.leaf_flag == LeafFlag::BUCKET {
            Err(CursorError::ValueIsABucket.into())
          } else {
            Err(CursorError::ValueIsBytes.into())
          }
        }
      }
    }
  }
}

impl<C> CoreCursorRefApi for LeafFlagFilterCursor<C>
where
  C: CoreCursorRefApi,
{
  type KvRef<'a>
    = C::KvRef<'a>
  where
    Self: 'a;

  #[inline]
  fn key_ref<'a>(&'a self) -> Option<Self::KvRef<'a>> {
    self.cursor.key_ref()
  }

  #[inline]
  fn value_ref<'a>(&'a self) -> Option<Self::KvRef<'a>> {
    self.cursor.value_ref()
  }

  #[inline]
  fn key_value_ref<'a>(&'a self) -> Option<(Self::KvRef<'a>, Self::KvRef<'a>)> {
    self.cursor.key_value_ref()
  }
}

impl<'tx, C> CoreCursorApi<'tx> for LeafFlagFilterCursor<C>
where
  C: CoreCursorApi<'tx>,
{
  type KvTx = C::KvTx;

  #[inline]
  fn key(&self) -> Option<Self::KvTx> {
    self.cursor.key()
  }

  #[inline]
  fn value(&self) -> Option<Self::KvTx> {
    self.cursor.value()
  }

  #[inline]
  fn key_value(&self) -> Option<(Self::KvTx, Self::KvTx)> {
    self.cursor.key_value()
  }
}

pub trait CursorRefApi: for<'a> GatKvRef<'a> {
  fn first_ref<'a>(
    &'a mut self,
  ) -> crate::Result<
    Option<(<Self as GatKvRef<'a>>::KvRef, <Self as GatKvRef<'a>>::KvRef)>,
    CursorError,
  >;
  fn next_ref<'a>(
    &'a mut self,
  ) -> crate::Result<
    Option<(<Self as GatKvRef<'a>>::KvRef, <Self as GatKvRef<'a>>::KvRef)>,
    CursorError,
  >;
  fn prev_ref<'a>(
    &'a mut self,
  ) -> crate::Result<
    Option<(<Self as GatKvRef<'a>>::KvRef, <Self as GatKvRef<'a>>::KvRef)>,
    CursorError,
  >;
  fn last_ref<'a>(
    &'a mut self,
  ) -> crate::Result<
    Option<(<Self as GatKvRef<'a>>::KvRef, <Self as GatKvRef<'a>>::KvRef)>,
    CursorError,
  >;
  fn seek_ref<'a>(
    &'a mut self, v: &[u8],
  ) -> crate::Result<
    Option<(<Self as GatKvRef<'a>>::KvRef, <Self as GatKvRef<'a>>::KvRef)>,
    CursorError,
  >;
}

#[derive(Clone)]
pub struct CursorIter<'tx, C> {
  cursor: C,
  started: bool,
  _tx: PhantomData<&'tx ()>,
}

impl<'tx, C> Iterator for CursorIter<'tx, C>
where
  C: CursorApi<'tx>,
{
  type Item = crate::Result<(C::KvTx, C::KvTx), CursorError>;

  fn next(&mut self) -> Option<Self::Item> {
    match if !self.started {
      self.started = true;
      self.cursor.first()
    } else {
      self.cursor.next()
    } {
      Ok(Some((key, val))) => Some(Ok((key, val))),
      Ok(None) => None,
      Err(err) => Some(Err(err)),
    }
  }
}

pub trait CursorApi<'tx>: Clone {
  type KvTx: GetKvTxSlice<'tx>;

  fn first(&mut self) -> crate::Result<Option<(Self::KvTx, Self::KvTx)>, CursorError>;
  fn next(&mut self) -> crate::Result<Option<(Self::KvTx, Self::KvTx)>, CursorError>;
  fn prev(&mut self) -> crate::Result<Option<(Self::KvTx, Self::KvTx)>, CursorError>;
  fn last(&mut self) -> crate::Result<Option<(Self::KvTx, Self::KvTx)>, CursorError>;
  fn seek(&mut self, key: &[u8]) -> crate::Result<Option<(Self::KvTx, Self::KvTx)>, CursorError>;
}

pub trait CursorLeafApi<'tx> {
  type LeafType: HasLeaves<'tx>;

  fn move_to_prev_leaf(&mut self) -> crate::Result<Option<LeafFlag>, CursorError>;
  fn move_to_next_leaf(&mut self) -> crate::Result<Option<LeafFlag>, CursorError>;
  fn seek_leaf(&mut self, key: &[u8]) -> Option<&Self::LeafType>;
}

pub enum CursorMutKv<D> {
  OnDisk(D),
  Upsert(SharedData),
}

impl<D> AsRef<[u8]> for CursorMutKv<D>
where
  D: AsRef<[u8]>,
{
  fn as_ref(&self) -> &[u8] {
    match self {
      CursorMutKv::OnDisk(d) => d.as_ref(),
      CursorMutKv::Upsert(u) => u.as_ref(),
    }
  }
}

impl<D> Deref for CursorMutKv<D>
where
  D: AsRef<[u8]>,
{
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    self.as_ref()
  }
}

/*
pub struct CursorMutIter<'tx, C> {

}*/

pub struct RefTxCursor<'tx, TX: TheTx<'tx, TxPageType = DirectPage<'tx, RefTxBytes<'tx>>>> {
  cursor: LeafFlagFilterCursor<CoreCursor<'tx, TX::BranchType, TX::LeafType, TX>>,
}

impl<'tx, TX: TheTx<'tx, TxPageType = DirectPage<'tx, RefTxBytes<'tx>>>> Clone for RefTxCursor<'tx, TX> {
  fn clone(&self) -> Self {
    RefTxCursor {
      cursor: self.cursor.clone(),
    }
  }
}

impl<'a, 'tx, TX: TheTx<'tx, TxPageType = DirectPage<'tx, RefTxBytes<'tx>>>> GatKvRef<'a>
  for RefTxCursor<'tx, TX>
{
  type KvRef = <TX::LeafType as GatKvRef<'a>>::KvRef;
}

impl<'tx, TX: TheTx<'tx, TxPageType = DirectPage<'tx, RefTxBytes<'tx>>>> CursorRefApi
  for RefTxCursor<'tx, TX>
where
  for<'b> <TX::BranchType as GatKvRef<'b>>::KvRef: PartialOrd<[u8]>,
  for<'b> <TX::LeafType as GatKvRef<'b>>::KvRef: PartialOrd<[u8]>,
{
  fn first_ref<'a>(
    &'a mut self,
  ) -> crate::Result<
    Option<(<Self as GatKvRef<'a>>::KvRef, <Self as GatKvRef<'a>>::KvRef)>,
    CursorError,
  > {
    Ok(
      self
        .cursor
        .move_to_first_element()?
        .map(move |_| self.cursor.key_value_ref())
        .flatten(),
    )
  }

  fn next_ref<'a>(
    &'a mut self,
  ) -> crate::Result<
    Option<(<Self as GatKvRef<'a>>::KvRef, <Self as GatKvRef<'a>>::KvRef)>,
    CursorError,
  > {
    Ok(
      self
        .cursor
        .move_to_next_element()?
        .map(move |_| self.cursor.key_value_ref())
        .flatten(),
    )
  }

  fn prev_ref<'a>(
    &'a mut self,
  ) -> crate::Result<
    Option<(<Self as GatKvRef<'a>>::KvRef, <Self as GatKvRef<'a>>::KvRef)>,
    CursorError,
  > {
    Ok(
      self
        .cursor
        .move_to_prev_element()?
        .map(move |_| self.cursor.key_value_ref())
        .flatten(),
    )
  }

  fn last_ref<'a>(
    &'a mut self,
  ) -> crate::Result<
    Option<(<Self as GatKvRef<'a>>::KvRef, <Self as GatKvRef<'a>>::KvRef)>,
    CursorError,
  > {
    Ok(
      self
        .cursor
        .move_to_last_element()?
        .map(move |_| self.cursor.key_value_ref())
        .flatten(),
    )
  }

  fn seek_ref<'a>(
    &'a mut self, v: &[u8],
  ) -> crate::Result<
    Option<(<Self as GatKvRef<'a>>::KvRef, <Self as GatKvRef<'a>>::KvRef)>,
    CursorError,
  > {
    Ok(
      self
        .cursor
        .seek(v)?
        .map(move |_| self.cursor.key_value_ref())
        .flatten(),
    )
  }
}

impl<'tx, TX: TheTx<'tx, TxPageType = DirectPage<'tx, RefTxBytes<'tx>>>> CursorApi<'tx>
  for RefTxCursor<'tx, TX>
where
  for<'b> <TX::BranchType as GatKvRef<'b>>::KvRef: PartialOrd<[u8]>,
  for<'b> <TX::LeafType as GatKvRef<'b>>::KvRef: PartialOrd<[u8]>,
{
  type KvTx = <TX::LeafType as HasKeys<'tx>>::TxKv;

  fn first(&mut self) -> crate::Result<Option<(Self::KvTx, Self::KvTx)>, CursorError> {
    Ok(
      self
        .cursor
        .move_to_first_element()?
        .map(move |_| self.cursor.key_value())
        .flatten(),
    )
  }

  fn next(&mut self) -> crate::Result<Option<(Self::KvTx, Self::KvTx)>, CursorError> {
    Ok(
      self
        .cursor
        .move_to_next_element()?
        .map(move |_| self.cursor.key_value())
        .flatten(),
    )
  }

  fn prev(&mut self) -> crate::Result<Option<(Self::KvTx, Self::KvTx)>, CursorError> {
    Ok(
      self
        .cursor
        .move_to_prev_element()?
        .map(move |_| self.cursor.key_value())
        .flatten(),
    )
  }

  fn last(&mut self) -> crate::Result<Option<(Self::KvTx, Self::KvTx)>, CursorError> {
    Ok(
      self
        .cursor
        .move_to_last_element()?
        .map(move |_| self.cursor.key_value())
        .flatten(),
    )
  }

  fn seek(&mut self, v: &[u8]) -> crate::Result<Option<(Self::KvTx, Self::KvTx)>, CursorError> {
    Ok(
      self
        .cursor
        .seek(v)?
        .map(move |_| self.cursor.key_value())
        .flatten(),
    )
  }
}

pub struct LazyTxCursor<'tx, TX: TheLazyTx<'tx, TxPageType = LazyPage<'tx, TX>>> {
  cursor: LeafFlagFilterCursor<CoreCursor<'tx, TX::BranchType, TX::LeafType, TX>>,
}

impl<'tx, TX: TheLazyTx<'tx, TxPageType = LazyPage<'tx, TX>>> Clone for LazyTxCursor<'tx, TX> {
  fn clone(&self) -> Self {
    LazyTxCursor {
      cursor: self.cursor.clone(),
    }
  }
}

impl<'a, 'tx, TX: TheLazyTx<'tx, TxPageType = LazyPage<'tx, TX>>> GatKvRef<'a>
  for LazyTxCursor<'tx, TX>
{
  type KvRef = <TX::LeafType as GatKvRef<'a>>::KvRef;
}

impl<'tx, TX: TheLazyTx<'tx, TxPageType = LazyPage<'tx, TX>>> CursorRefApi for LazyTxCursor<'tx, TX>
where
  for<'b> <TX::BranchType as GatKvRef<'b>>::KvRef: TryPartialOrd<[u8]>,
  for<'b> <TX::LeafType as GatKvRef<'b>>::KvRef: TryPartialOrd<[u8]>,
{
  fn first_ref<'a>(
    &'a mut self,
  ) -> crate::Result<
    Option<(<Self as GatKvRef<'a>>::KvRef, <Self as GatKvRef<'a>>::KvRef)>,
    CursorError,
  > {
    Ok(
      self
        .cursor
        .move_to_first_element()?
        .map(move |_| self.cursor.key_value_ref())
        .flatten(),
    )
  }

  fn next_ref<'a>(
    &'a mut self,
  ) -> crate::Result<
    Option<(<Self as GatKvRef<'a>>::KvRef, <Self as GatKvRef<'a>>::KvRef)>,
    CursorError,
  > {
    Ok(
      self
        .cursor
        .move_to_next_element()?
        .map(move |_| self.cursor.key_value_ref())
        .flatten(),
    )
  }

  fn prev_ref<'a>(
    &'a mut self,
  ) -> crate::Result<
    Option<(<Self as GatKvRef<'a>>::KvRef, <Self as GatKvRef<'a>>::KvRef)>,
    CursorError,
  > {
    Ok(
      self
        .cursor
        .move_to_prev_element()?
        .map(move |_| self.cursor.key_value_ref())
        .flatten(),
    )
  }

  fn last_ref<'a>(
    &'a mut self,
  ) -> crate::Result<
    Option<(<Self as GatKvRef<'a>>::KvRef, <Self as GatKvRef<'a>>::KvRef)>,
    CursorError,
  > {
    Ok(
      self
        .cursor
        .move_to_last_element()?
        .map(move |_| self.cursor.key_value_ref())
        .flatten(),
    )
  }

  fn seek_ref<'a>(
    &'a mut self, v: &[u8],
  ) -> crate::Result<
    Option<(<Self as GatKvRef<'a>>::KvRef, <Self as GatKvRef<'a>>::KvRef)>,
    CursorError,
  > {
    Ok(
      self
        .cursor
        .try_seek(v)?
        .map(move |_| self.cursor.key_value_ref())
        .flatten(),
    )
  }
}

impl<'tx, TX: TheLazyTx<'tx, TxPageType = LazyPage<'tx, TX>>> CursorApi<'tx>
  for LazyTxCursor<'tx, TX>
where
  for<'b> <TX::BranchType as GatKvRef<'b>>::KvRef: TryPartialOrd<[u8]>,
  for<'b> <TX::LeafType as GatKvRef<'b>>::KvRef: TryPartialOrd<[u8]>,
{
  type KvTx = <TX::LeafType as HasKeys<'tx>>::TxKv;

  fn first(&mut self) -> crate::Result<Option<(Self::KvTx, Self::KvTx)>, CursorError> {
    Ok(
      self
        .cursor
        .move_to_first_element()?
        .map(move |_| self.cursor.key_value())
        .flatten(),
    )
  }

  fn next(&mut self) -> crate::Result<Option<(Self::KvTx, Self::KvTx)>, CursorError> {
    Ok(
      self
        .cursor
        .move_to_next_element()?
        .map(move |_| self.cursor.key_value())
        .flatten(),
    )
  }

  fn prev(&mut self) -> crate::Result<Option<(Self::KvTx, Self::KvTx)>, CursorError> {
    Ok(
      self
        .cursor
        .move_to_prev_element()?
        .map(move |_| self.cursor.key_value())
        .flatten(),
    )
  }

  fn last(&mut self) -> crate::Result<Option<(Self::KvTx, Self::KvTx)>, CursorError> {
    Ok(
      self
        .cursor
        .move_to_last_element()?
        .map(move |_| self.cursor.key_value())
        .flatten(),
    )
  }

  fn seek(&mut self, v: &[u8]) -> crate::Result<Option<(Self::KvTx, Self::KvTx)>, CursorError> {
    Ok(
      self
        .cursor
        .try_seek(v)?
        .map(move |_| self.cursor.key_value())
        .flatten(),
    )
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::api::tx::TxStats;
  use crate::common::buffer_pool::BufferPool;
  use crate::common::layout::bucket::BucketHeader;
  use crate::common::vec_pool::VecPool;
  use crate::components::tx::{CoreTxHandle, LazyTxHandle, RefTxHandle};
  use crate::io::backends::file::{
    FileReadOptions, MultiFileIO, MultiFileReadOptions, SingleFileIO,
  };
  use crate::io::backends::memmap::{MemMapIO, MemMapReadOptions};
  use crate::io::backends::meta_reader::MetaReader;
  use crate::io::backends::p_file::{PFileIO, PFileReadOptions};
  use crate::io::backends::{CachedReadHandler, DirectReadHandler, NewIOReader, ROShell};
  use crate::io::pages::lazy::ops::RefIntoTryBuf;
  use crate::io::pages::lazy::ops::TryBuf;
  use crate::io::transmogrify::direct::DirectTransmogrify;
  use bytemuck::bytes_of_mut;
  use memmap2::{Advice, Mmap, MmapOptions};
  use moka::sync::Cache;
  use parking_lot::RwLock;
  use size::Size;
  use std::fs::File;
  use std::io::{BufReader, BufWriter, Write};
  use std::path::PathBuf;
  use std::sync;
  use std::time::Instant;

  #[test]
  fn test_file() {
    let mut reader = BufReader::new(File::open("my.db").unwrap());
    let metadata = MetaReader::new(reader).determine_file_meta().unwrap();
    println!("{:?}", metadata);
    let meta = metadata.meta;
    let tx_id = meta.tx_id;
    let page_size = meta.page_size as usize;
    let root_page = meta.root.root();
    let buffer_pool = BufferPool::new(
      page_size,
      Size::from_megabytes(64),
      Size::from_megabytes(32),
      Size::from_megabytes(256),
    );
    let tx_stats = sync::Arc::new(TxStats::default());
    let path = sync::Arc::new(PathBuf::from("./my.db"));
    let read_options = FileReadOptions::new(buffer_pool.clone());
    let backend = SingleFileIO::new_ro(path, page_size, read_options).unwrap();
    let tx_context = DirectTransmogrify {};
    let handler = DirectReadHandler {
      tx_context,
      io: backend,
    };
    let cached_read_handler = RwLock::new(CachedReadHandler {
      handler,
      page_cache: Cache::new(10_000),
    });
    let read_lock = cached_read_handler.read();
    let core_tx = CoreTxHandle {
      io: read_lock.into(),
      stats: tx_stats.clone(),
      tx_id,
    };
    let tx = sync::Arc::new(LazyTxHandle { handle: core_tx });
    let root = tx.read_node_page(root_page.into()).unwrap();
    let stack_pool = VecPool::new(10, 5, 5_000);
    let bucket = OnDiskBucket {
      tx: tx.clone(),
      stack_pool: stack_pool.clone(),
      header: Default::default(),
      root,
    };
    let mut cursor = LazyTxCursor {
      cursor: LeafFlagFilterCursor {
        cursor: CoreCursor::new_with_stack(&bucket, stack_pool.pop()),
        leaf_flag: LeafFlag::BUCKET,
      },
    };
    let kv = cursor.seek_ref(b"dict".as_slice()).expect("no_errors");
    let (k, v) = kv.unwrap();
    let mut k_buf = k.ref_into_try_buf().unwrap();
    println!("{:?}", k_buf.remaining());
    println!("{:?}", k_buf.chunk());
    k_buf.try_advance(4).unwrap();
    println!("{:?}", k_buf.remaining());
    let mut v_buf = v.ref_into_try_buf().unwrap();
    println!("{:?}", v_buf.remaining());
    let mut bucket_header = BucketHeader::default();
    bytes_of_mut(&mut bucket_header).copy_from_slice(v_buf.chunk());
    println!("{:?}", bucket_header);

    let dict_root = tx.read_node_page(bucket_header.root().into()).unwrap();
    let dict_bucket = OnDiskBucket {
      tx: tx.clone(),
      stack_pool: stack_pool.clone(),
      header: Default::default(),
      root: dict_root,
    };
    let now = Instant::now();
    for _ in 0..5_000 {
      let mut dict_cursor = LazyTxCursor {
        cursor: LeafFlagFilterCursor {
          cursor: CoreCursor::new_with_stack(&dict_bucket, stack_pool.pop()),
          leaf_flag: LeafFlag::empty(),
        },
      };
      let _ = dict_cursor.first_ref().unwrap();
      loop {
        match dict_cursor.next_ref().unwrap() {
          Some(_) => continue,
          None => break,
        }
      }
      /*      let mut dict_iter = CursorIter {
        cursor: dict_cursor,
        started: false,
        _tx: Default::default(),
      };
      for result in dict_iter {
        let (_k, _v) = result.unwrap();
      }*/
    }
    println!("file: {:?}", now.elapsed());
    /*
    let dict_cursor = LazyTxCursor {
      cursor: LeafFlagFilterCursor {
        cursor: CoreCursor::new_with_stack(&dict_bucket, stack_pool.pop()),
        leaf_flag: LeafFlag::empty(),
      },
    };
    let mut dict_iter = CursorIter {
      cursor: dict_cursor,
      started: false,
      _tx: Default::default(),
    };
    let mut write = BufWriter::new(File::create("out_file.csv").unwrap());
    let now = Instant::now();
    for result in dict_iter {
      let (k, v) = result.unwrap();
      let mut k_buf = k.ref_into_try_buf().unwrap();
      let v_buf = v.ref_into_try_buf().unwrap();
      let k_string = String::from_utf8_lossy(k_buf.chunk());
      write
        .write_fmt(format_args!("{},{}\n", k_string, v_buf.remaining()))
        .unwrap();
    }
    write.flush().unwrap();
    println!("file: {:?}", now.elapsed());*/
  }

  #[test]
  fn test_pfile() {
    let mut reader = BufReader::new(File::open("my.db").unwrap());
    let metadata = MetaReader::new(reader).determine_file_meta().unwrap();
    println!("{:?}", metadata);
    let meta = metadata.meta;
    let tx_id = meta.tx_id;
    let page_size = meta.page_size as usize;
    let root_page = meta.root.root();
    let buffer_pool = BufferPool::new(
      page_size,
      Size::from_megabytes(64),
      Size::from_megabytes(32),
      Size::from_megabytes(256),
    );
    let tx_stats = sync::Arc::new(TxStats::default());
    let path = sync::Arc::new(PathBuf::from("./my.db"));
    let read_options = PFileReadOptions::new(buffer_pool.clone());
    let backend = PFileIO::new_ro(path, page_size, read_options).unwrap();
    let tx_context = DirectTransmogrify {};
    let handler = DirectReadHandler {
      tx_context,
      io: backend,
    };
    let cached_read_handler = RwLock::new(CachedReadHandler {
      handler,
      page_cache: Cache::new(10_000),
    });
    let read_lock = cached_read_handler.read();
    let core_tx = CoreTxHandle {
      io: read_lock.into(),
      stats: tx_stats.clone(),
      tx_id,
    };
    let tx = sync::Arc::new(LazyTxHandle { handle: core_tx });
    let root = tx.read_node_page(root_page.into()).unwrap();
    let stack_pool = VecPool::new(10, 5, 5_000);
    let bucket = OnDiskBucket {
      tx: tx.clone(),
      stack_pool: stack_pool.clone(),
      header: Default::default(),
      root,
    };
    let mut cursor = LazyTxCursor {
      cursor: LeafFlagFilterCursor {
        cursor: CoreCursor::new_with_stack(&bucket, stack_pool.pop()),
        leaf_flag: LeafFlag::BUCKET,
      },
    };
    let kv = cursor.seek_ref(b"dict".as_slice()).expect("no_errors");
    let (k, v) = kv.unwrap();
    let mut k_buf = k.ref_into_try_buf().unwrap();
    println!("{:?}", k_buf.remaining());
    println!("{:?}", k_buf.chunk());
    k_buf.try_advance(4).unwrap();
    println!("{:?}", k_buf.remaining());
    let mut v_buf = v.ref_into_try_buf().unwrap();
    println!("{:?}", v_buf.remaining());
    let mut bucket_header = BucketHeader::default();
    bytes_of_mut(&mut bucket_header).copy_from_slice(v_buf.chunk());
    println!("{:?}", bucket_header);

    let dict_root = tx.read_node_page(bucket_header.root().into()).unwrap();
    let dict_bucket = OnDiskBucket {
      tx: tx.clone(),
      stack_pool: stack_pool.clone(),
      header: Default::default(),
      root: dict_root,
    };
    let now = Instant::now();
    for _ in 0..5_000 {
      let mut dict_cursor = LazyTxCursor {
        cursor: LeafFlagFilterCursor {
          cursor: CoreCursor::new_with_stack(&dict_bucket, stack_pool.pop()),
          leaf_flag: LeafFlag::empty(),
        },
      };
      let _ = dict_cursor.first_ref().unwrap();
      loop {
        match dict_cursor.next_ref().unwrap() {
          Some(_) => continue,
          None => break,
        }
      }
      /*      let mut dict_iter = CursorIter {
        cursor: dict_cursor,
        started: false,
        _tx: Default::default(),
      };
      for result in dict_iter {
        let (_k, _v) = result.unwrap();
      }*/
    }
    println!("pfile: {:?}", now.elapsed()); /*
    let dict_cursor = LazyTxCursor {
    cursor: LeafFlagFilterCursor {
    cursor: CoreCursor::new_with_stack(&dict_bucket, stack_pool.pop()),
    leaf_flag: LeafFlag::empty(),
    },
    };
    let mut dict_iter = CursorIter {
    cursor: dict_cursor,
    started: false,
    _tx: Default::default(),
    };
    let mut write = BufWriter::new(File::create("out_file.csv").unwrap());
    let now = Instant::now();
    for result in dict_iter {
    let (k, v) = result.unwrap();
    let mut k_buf = k.ref_into_try_buf().unwrap();
    let v_buf = v.ref_into_try_buf().unwrap();
    let k_string = String::from_utf8_lossy(k_buf.chunk());
    write
    .write_fmt(format_args!("{},{}\n", k_string, v_buf.remaining()))
    .unwrap();
    }
    write.flush().unwrap();
    println!("pfile: {:?}", now.elapsed());*/
  }

  #[test]
  fn test_multifile() {
    let mut reader = BufReader::new(File::open("my.db").unwrap());
    let metadata = MetaReader::new(reader).determine_file_meta().unwrap();
    println!("{:?}", metadata);
    let meta = metadata.meta;
    let tx_id = meta.tx_id;
    let page_size = meta.page_size as usize;
    let root_page = meta.root.root();
    let buffer_pool = BufferPool::new(
      page_size,
      Size::from_megabytes(64),
      Size::from_megabytes(32),
      Size::from_megabytes(256),
    );
    let tx_stats = sync::Arc::new(TxStats::default());
    let path = sync::Arc::new(PathBuf::from("./my.db"));
    let options = MultiFileReadOptions::new(buffer_pool.clone(), 10);
    let backend = MultiFileIO::new_ro(path.clone(), page_size, options).unwrap();
    let tx_context = DirectTransmogrify {};
    let handler = DirectReadHandler {
      tx_context,
      io: backend,
    };
    let cached_read_handler = RwLock::new(CachedReadHandler {
      handler,
      page_cache: Cache::new(10_000),
    });
    let read_lock = cached_read_handler.read();
    let core_tx = CoreTxHandle {
      io: read_lock.into(),
      stats: tx_stats.clone(),
      tx_id,
    };
    let tx = sync::Arc::new(LazyTxHandle { handle: core_tx });
    let root = tx.read_node_page(root_page.into()).unwrap();
    let stack_pool = VecPool::new(10, 5, 5_000);
    let bucket = OnDiskBucket {
      tx: tx.clone(),
      stack_pool: stack_pool.clone(),
      header: Default::default(),
      root,
    };
    let mut cursor = LazyTxCursor {
      cursor: LeafFlagFilterCursor {
        cursor: CoreCursor::new_with_stack(&bucket, stack_pool.pop()),
        leaf_flag: LeafFlag::BUCKET,
      },
    };
    let kv = cursor.seek_ref(b"dict".as_slice()).expect("no_errors");
    let (k, v) = kv.unwrap();
    let mut k_buf = k.ref_into_try_buf().unwrap();
    println!("{:?}", k_buf.remaining());
    println!("{:?}", k_buf.chunk());
    k_buf.try_advance(4).unwrap();
    println!("{:?}", k_buf.remaining());
    let mut v_buf = v.ref_into_try_buf().unwrap();
    println!("{:?}", v_buf.remaining());
    let mut bucket_header = BucketHeader::default();
    bytes_of_mut(&mut bucket_header).copy_from_slice(v_buf.chunk());
    println!("{:?}", bucket_header);

    let dict_root = tx.read_node_page(bucket_header.root().into()).unwrap();
    let dict_bucket = OnDiskBucket {
      tx: tx.clone(),
      stack_pool: stack_pool.clone(),
      header: Default::default(),
      root: dict_root,
    };
    let now = Instant::now();
    for _ in 0..5_000 {
      let mut dict_cursor = LazyTxCursor {
        cursor: LeafFlagFilterCursor {
          cursor: CoreCursor::new_with_stack(&dict_bucket, stack_pool.pop()),
          leaf_flag: LeafFlag::empty(),
        },
      };
      let _ = dict_cursor.first_ref().unwrap();
      loop {
        match dict_cursor.next_ref().unwrap() {
          Some(_) => continue,
          None => break,
        }
      }
      /*      let mut dict_iter = CursorIter {
        cursor: dict_cursor,
        started: false,
        _tx: Default::default(),
      };
      for result in dict_iter {
        let (_k, _v) = result.unwrap();
      }*/
    }
    println!("multifile: {:?}", now.elapsed());

    /*
    let dict_cursor = LazyTxCursor {
      cursor: LeafFlagFilterCursor {
        cursor: CoreCursor::new_with_stack(&dict_bucket, stack_pool.pop()),
        leaf_flag: LeafFlag::empty(),
      },
    };
    let mut dict_iter = CursorIter {
      cursor: dict_cursor,
      started: false,
      _tx: Default::default(),
    };
    let mut write = BufWriter::new(File::create("out_multi.csv").unwrap());
    let now = Instant::now();
    for result in dict_iter {
      let (k, v) = result.unwrap();
      let mut k_buf = k.ref_into_try_buf().unwrap();
      let v_buf = v.ref_into_try_buf().unwrap();
      let k_string = String::from_utf8_lossy(k_buf.chunk());
      write
        .write_fmt(format_args!("{},{}\n", k_string, v_buf.remaining()))
        .unwrap();
    }
    write.flush().unwrap();
    println!("multifile: {:?}", now.elapsed());*/
  }

  #[test]
  fn test_memmap() {
    let mut reader = BufReader::new(File::open("my.db").unwrap());
    let metadata = MetaReader::new(reader).determine_file_meta().unwrap();
    println!("{:?}", metadata);
    let meta = metadata.meta;
    let tx_id = meta.tx_id;
    let page_size = meta.page_size as usize;
    let root_page = meta.root.root();
    let buffer_pool = BufferPool::new(
      page_size,
      Size::from_kibibytes(64),
      Size::from_kibibytes(32),
      Size::from_kibibytes(256),
    );
    let tx_stats = sync::Arc::new(TxStats::default());
    let path = sync::Arc::new(PathBuf::from("./my.db"));
    let options = MemMapReadOptions::new(true, true, true);
    let backend = MemMapIO::new_ro(path.clone(), page_size, options).unwrap();
    let tx_context = DirectTransmogrify {};
    let handler = DirectReadHandler {
      tx_context,
      io: backend,
    };
    let cached_read_handler = RwLock::new(handler);
    let read_lock = cached_read_handler.read();
    let core_tx = CoreTxHandle {
      io: read_lock.into(),
      stats: tx_stats.clone(),
      tx_id,
    };
    let tx = sync::Arc::new(RefTxHandle { handle: core_tx });
    let root = tx.read_node_page(root_page.into()).unwrap();
    let stack_pool = VecPool::new(10, 5, 5_000);
    let bucket = OnDiskBucket {
      tx: tx.clone(),
      stack_pool: stack_pool.clone(),
      header: Default::default(),
      root,
    };
    let mut cursor = RefTxCursor {
      cursor: LeafFlagFilterCursor {
        cursor: CoreCursor::new_with_stack(&bucket, stack_pool.pop()),
        leaf_flag: LeafFlag::BUCKET,
      },
    };
    let kv = cursor.seek_ref(b"dict".as_slice()).expect("no_errors");
    let (k, v) = kv.unwrap();
    let mut k_buf = k.ref_into_try_buf().unwrap();
    println!("{:?}", k_buf.remaining());
    println!("{:?}", k_buf.chunk());
    k_buf.try_advance(4).unwrap();
    println!("{:?}", k_buf.remaining());
    let mut v_buf = v.ref_into_try_buf().unwrap();
    println!("{:?}", v_buf.remaining());
    let mut bucket_header = BucketHeader::default();
    bytes_of_mut(&mut bucket_header).copy_from_slice(v_buf.chunk());
    println!("{:?}", bucket_header);

    let dict_root = tx.read_node_page(bucket_header.root().into()).unwrap();
    let dict_bucket = OnDiskBucket {
      tx: tx.clone(),
      stack_pool: stack_pool.clone(),
      header: Default::default(),
      root: dict_root,
    };
    let now = Instant::now();
    for _ in 0..5_000 {
      let mut dict_cursor = RefTxCursor {
        cursor: LeafFlagFilterCursor {
          cursor: CoreCursor::new_with_stack(&dict_bucket, stack_pool.pop()),
          leaf_flag: LeafFlag::empty(),
        },
      };
      let _ = dict_cursor.first_ref().unwrap();
      loop {
        match dict_cursor.next_ref().unwrap() {
          Some(_) => continue,
          None => break,
        }
      }
      /*      let mut dict_iter = CursorIter {
        cursor: dict_cursor,
        started: false,
        _tx: Default::default(),
      };
      for result in dict_iter {
        let (_k, _v) = result.unwrap();
      }*/
    }
    println!("memmap: {:?}", now.elapsed());
    /*let dict_cursor = RefTxCursor {
      cursor: LeafFlagFilterCursor {
        cursor: CoreCursor::new_with_stack(&dict_bucket, stack_pool.pop()),
        leaf_flag: LeafFlag::empty(),
      },
    };
    let mut dict_iter = CursorIter {
      cursor: dict_cursor,
      started: false,
      _tx: Default::default(),
    };
    let mut write = BufWriter::new(File::create("out_memmap.csv").unwrap());
    let now = Instant::now();
    for result in dict_iter {
      let (k, v) = result.unwrap();
      let mut k_buf = k.ref_into_try_buf().unwrap();
      let v_buf = v.ref_into_try_buf().unwrap();
      let k_string = String::from_utf8_lossy(k_buf.chunk());
      write
        .write_fmt(format_args!("{},{}\n", k_string, v_buf.remaining()))
        .unwrap();
    }
    write.flush().unwrap();
    println!("memmap: {:?}", now.elapsed());*/
  }
}
