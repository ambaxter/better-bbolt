use crate::common::id::{DiskPageId, FreelistPageId, MetaPageId, NodePageId};
use crate::tx_io::transmogrify::{TxContext, TxDirectContext, TxIndirectContext};

struct DirectTransmogrify;

impl TxContext for DirectTransmogrify {
  #[inline]
  fn trans_meta_id(&self, meta_page_id: MetaPageId) -> DiskPageId {
    DiskPageId(meta_page_id.0.0)
  }

  #[inline]
  fn trans_freelist_id(&self, freelist_page_id: FreelistPageId) -> DiskPageId {
    DiskPageId(freelist_page_id.0.0)
  }

  #[inline]
  fn trans_node_id(&self, node_page_id: NodePageId) -> DiskPageId {
    DiskPageId(node_page_id.0.0)
  }
}

impl TxDirectContext for DirectTransmogrify {}

impl TxIndirectContext for DirectTransmogrify {
  #[inline]
  fn trans_freelist_overflow(&self, freelist_page_id: FreelistPageId, overflow: u32) -> DiskPageId {
    let page_id = freelist_page_id + overflow;
    DiskPageId(page_id.0.0)
  }

  #[inline]
  fn trans_node_overflow(&self, node_page_id: NodePageId, overflow: u32) -> DiskPageId {
    let page_id = node_page_id + overflow;
    DiskPageId(page_id.0.0)
  }
}
