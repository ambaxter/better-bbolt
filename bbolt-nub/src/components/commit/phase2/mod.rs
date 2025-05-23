mod doc;

use crate::common::data_pool::SharedData;
use crate::common::errors::CursorError;
use crate::common::id::{NodePageId, WipNodeGenerator, WipNodeId};
use crate::common::layout::node::LeafFlag;
use crate::components::bucket::ValueDelta;
use crate::components::cursor::CoreCursor;
use crate::components::tx::TheTx;
use crate::io::TxSlot;
use crate::io::pages::{GatKvRef, GetKvTxSlice, TxPageType};
use hashbrown::HashMap;
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CommitError {
  #[error("CommitError: Unspecified Failure")]
  Failure,
}

pub struct WipNode {
  wip_id: WipNodeId,
  parent_wip_id: Option<WipNodeId>,
  node_page_id: Option<NodePageId>,
  parent_node_page_id: Option<NodePageId>,
  entries: BTreeMap<SharedData, WipNodeId>,
}

pub enum LeafValue<D> {
  OnDisk(D),
  Upsert(SharedData),
}

pub struct WipLeaf<D> {
  wip_id: WipNodeId,
  first_key: SharedData,
  entries: BTreeMap<SharedData, LeafValue<D>>,
}

pub struct WipCommit<'tx, D> {
  wip_node_generator: WipNodeGenerator,
  wip_nodes: HashMap<WipNodeId, WipNode>,
  wip_leaves: BTreeMap<SharedData, WipLeaf<D>>,
  tx_slot: TxSlot<'tx>,
}

impl<'tx, TX> WipCommit<'tx, <TX::TxPageType as GetKvTxSlice<'tx>>::KvTx>
where
  TX: TheTx<'tx>,
{

  pub fn new_bucket<F>(
    mut delta: BTreeMap<SharedData, ValueDelta>) {

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
