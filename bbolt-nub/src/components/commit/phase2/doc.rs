// For each index, starting with the "last":

// I have Index nodes on disk
// I have an Index delta
// I need
// * A place to store the new node data
// * This place needs to be accessible either via old NodePadeId or new one
// * A leaf node iterator
//
// I distribute the delta amongst the work in progress (WIP) nodes
// I massage the WIP nodes to be the correct size
//

// What I want:
// * Work on leaf nodes with siblings and parents at once
// * Splitting a leaf node in place
// * Transferring entries from current node to left or right node as necessary
// * Sibling nodes don't need to be mutable to be part of the process, but may become mut when entries transferred to them

use crate::common::data_pool::SharedData;
use crate::common::id::{NodePageId, WipNodeId};
use hashbrown::HashMap;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

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

pub struct WipCommit<D> {
  wip_nodes: HashMap<WipNodeId, WipNode>,
  wip_leaves: BTreeMap<SharedData, WipLeaf<D>>,
}

// For each node, there are Option<LeftSibling>, Option<RightSibling>, Option<ParentBranch>
// Left and Right sibling need not be mutable if there's no updates
// If there are updates, updates must be applied so we can get an account on what's happening
// If updates fit *this* node and LeftSibling exists and LeftSibling is updated
// - Return Some(LeftSibling), then Some(ThisNode)
// If updates do not fit this node and LeftSibling exists, is mutable, and can successfully take entries where LeftSibling and ThisNode fit:
// - move entries to LeftSibling. Return Some(LeftSibling), then Some(ThisNode)
// If updates no not fit this node and LeftSibling isn't mut, try RightSibling if it's mut. If

// Iterate entries in reverse order. That way we automatically have the beginning and end of the node
