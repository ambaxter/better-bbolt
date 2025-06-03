mod doc;

use crate::common::data_pool::SharedData;
use crate::common::errors::CursorError;
use crate::common::id::{NodePageId, WipNodeGenerator, WipNodeId};
use crate::common::layout::node::{LeafElement, LeafFlag};
use crate::components::bucket::ValueDelta;
use crate::components::cursor::CoreCursor;
use crate::components::tx::TheTx;
use crate::io::TxSlot;
use crate::io::pages::{GatKvRef, GetKvTxSlice, TxPageType};
use hashbrown::HashMap;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::ops::Deref;
use thiserror::Error;
use crate::common::layout::page::PageHeader;

#[derive(Debug, Error)]
pub enum CommitError {
  #[error("CommitError: Unspecified Failure")]
  Failure,
}


pub enum LeafData<D> {
  OnDisk(D),
  Upsert(SharedData),
}

impl<D> Deref for LeafData<D> where D: Deref<Target = [u8]> {
  type Target = [u8];
  fn deref(&self) -> &Self::Target {
    match self {
      LeafData::OnDisk(data) => data,
      LeafData::Upsert(data) => data,
    }
  }
}

impl<D> AsRef<[u8]> for LeafData<D> where D: Deref<Target = [u8]> {
  fn as_ref(&self) -> &[u8] {
    self.deref()
  }
}

pub struct LeafValue<D> {
  data: LeafData<D>,
  is_bucket: bool,
}

impl<D> Deref for LeafValue<D> where D: Deref<Target = [u8]>
{
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    self.data.deref()
  }
}

impl<D> AsRef<[u8]> for LeafValue<D> where D: Deref<Target = [u8]> {
  fn as_ref(&self) -> &[u8] {
    self.data.deref()
  }
}

pub struct WipBranch {
  wip_id: WipNodeId,
  parent_wip_id: WipNodeId,
  first_key: SharedData,
  entries: BTreeMap<SharedData, WipNodeId>,
}

pub struct WipLeaf<D> {
  wip_id: WipNodeId,
  first_key: SharedData,
  entries: BTreeMap<SharedData, LeafValue<D>>,
}

pub struct WipLeafBuilder<D> {
  wip_id: WipNodeId,
  leaf: Option<WipLeaf<D>>,
  goal_byte_size: usize,
  current_byte_size: usize,
}

impl<D> WipLeafBuilder<D> {
  pub fn new(wip_id: WipNodeId, goal_page_size: usize) -> Self {
    WipLeafBuilder {
      wip_id,
      leaf: None,
      goal_byte_size: goal_page_size,
      current_byte_size: size_of::<PageHeader>(),
    }
  }

  #[inline]
  fn element_size(&self, key: &[u8], value: &[u8]) -> usize {
    size_of::<LeafElement>() + key.len() + value.len()
  }

  pub fn can_fit(&self, key: &[u8], value: &[u8]) -> bool {
    if self.leaf.is_none() {
      true
    } else {
      if self.current_byte_size + self.element_size(key, value) <= self.goal_byte_size {
        true
      } else {
        false
      }
    }
  }


  pub fn build(self) -> Option<WipLeaf<D>> {
    self.leaf
  }
}

impl<D> WipLeafBuilder<D> where D: Deref<Target = [u8]> {

  pub fn insert(&mut self, key: SharedData, value: LeafValue<D>) {
    self.current_byte_size += self.element_size(&key, &value);
    self.leaf = match self.leaf.take() {
      None => {
        let mut entries = BTreeMap::new();
        entries.insert(key.clone(), value);
        Some(WipLeaf {
          wip_id: self.wip_id,
          first_key: key,
          entries,
        })
      }
      Some(mut leaf) => {
        leaf.entries.insert(key, value);
        Some(leaf)
      }
    }
  }
}

pub struct WipCommit<'tx, TX: TheTx<'tx>> {
  wip_leaves: BTreeMap<SharedData, WipLeaf<<TX::TxPageType as GetKvTxSlice<'tx>>::KvTx>>,
  tx_type: PhantomData<&'tx TX>,
}

impl<'tx, TX: TheTx<'tx>> WipCommit<'tx, TX>
{
  pub fn from_new_index<F>(goal_page_size: usize,
    mut delta: BTreeMap<SharedData, ValueDelta>) {
    let mut wip_node_generator = WipNodeGenerator::new();
    let root_node = wip_node_generator.root();
    let mut child_parent_map = HashMap::new();
    let mut wip_leaves = BTreeMap::new();

    let mut leaf_builder = WipLeafBuilder::new(wip_node_generator.gen_next(), goal_page_size);
    for (key, value_delta) in delta {
      let (is_bucket, value) = match value_delta {
        ValueDelta::UValue(value) => (false, value),
        ValueDelta::UBucket(bucket) => (true, bucket),
        ValueDelta::Delete => continue,
      };
      if !leaf_builder.can_fit(&key, &value) {
        let leaf = leaf_builder.build().expect("Leaf builder building failed");
        child_parent_map.insert(leaf.wip_id, root_node);
        wip_leaves.insert(leaf.first_key.clone(), leaf);
        leaf_builder = WipLeafBuilder::new(wip_node_generator.gen_next(), goal_page_size);
      }
      leaf_builder.insert(key, LeafValue { data: LeafData::Upsert(value), is_bucket });
    }
    if let Some(leaf) = leaf_builder.build() {
      child_parent_map.insert(leaf.wip_id, root_node);
      wip_leaves.insert(leaf.first_key.clone(), leaf);
    }

    if wip_leaves.is_empty() {
      todo!("We should never have gotten here!")
    } else if wip_leaves.len() == 1 {
      todo!("The leaf is root!")
    } else {
      todo!("Build the tree")
    }
  }

  pub fn upsert_bucket<F>(
    mut bucket_cursor: CoreCursor<TX::BranchType, TX::LeafType, TX>,
    mut delta: BTreeMap<SharedData, ValueDelta>, seek: F,
  ) -> crate::Result<Self, CursorError>
  where
    F: FnMut(
      &mut CoreCursor<TX::BranchType, TX::LeafType, TX>,
      &[u8],
    ) -> crate::Result<Option<LeafFlag>, CursorError>,
  {
    let mut wip_node_generator = WipNodeGenerator::new();
    let wip_nodes = HashMap::new();
    let wip_leaves = BTreeMap::new();
    let tx_slot = TxSlot::default();

    // TODO: Don't die here
    let (last_key, _) = delta.last_key_value().unwrap();
    let seek_r = seek(&mut bucket_cursor, last_key)?;
    match seek_r {
      None => {todo!("all new bucket!")}
      Some(_) => {todo!("existing bucket!")}
    }
  }
}
