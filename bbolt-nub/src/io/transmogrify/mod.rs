use crate::common::id::{DiskPageId, FreelistPageId, MetaPageId, NodePageId};

pub mod direct;

//TODO: how do we handle when there is no full context yet? i.e. we haven't read the file yet?
pub trait TxContext {
  fn trans_meta_id(&self, meta_page_id: MetaPageId) -> DiskPageId;
  fn trans_freelist_id(&self, freelist_page_id: FreelistPageId) -> DiskPageId;
  fn trans_node_id(&self, node_page_id: NodePageId) -> DiskPageId;
}

pub trait TxDirectContext: TxContext {}

pub trait TxIndirectContext: TxContext {
  fn trans_freelist_overflow(&self, freelist_page_id: FreelistPageId, overflow: u32) -> DiskPageId;
  fn trans_node_overflow(&self, node_page_id: NodePageId, overflow: u32) -> DiskPageId;
}
