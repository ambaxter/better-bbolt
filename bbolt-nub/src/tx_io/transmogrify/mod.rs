use crate::common::id::{DiskPageId, FreelistPageId, MetaPageId, NodePageId};

pub mod direct;

pub trait TransDbIds {
  fn trans_meta_id(&self, meta_page_id: MetaPageId) -> DiskPageId;
  fn trans_freelist_id(&self, freelist_page_id: FreelistPageId) -> DiskPageId;
  fn trans_node_id(&self, node_page_id: NodePageId) -> DiskPageId;
}

pub trait TransComplete: TransDbIds {}

pub trait TransLazy: TransDbIds {
  fn trans_freelist_overflow(&self, freelist_page_id: FreelistPageId, overflow: u32) -> DiskPageId;
  fn trans_node_overflow(&self, node_page_id: NodePageId, overflow: u32) -> DiskPageId;
}
