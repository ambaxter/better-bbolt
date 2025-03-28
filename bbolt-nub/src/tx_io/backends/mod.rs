use crate::common::errors::DiskReadError;
use crate::common::id::{FreelistPageId, MetaPageId, NodePageId};
use crate::tx_io::bytes::IOBytes;

pub trait ReadIO {
  type Bytes: IOBytes;

  fn read_meta_page(&self, page_id: MetaPageId) -> crate::Result<Self::Bytes, DiskReadError>;

  fn read_freelist_page(
    &self, page_id: FreelistPageId,
  ) -> crate::Result<Self::Bytes, DiskReadError>;

  fn read_node_page(&self, page_id: NodePageId) -> crate::Result<Self::Bytes, DiskReadError>;
}

pub trait ReadCompleteIO: ReadIO {}

pub trait ReadLazyIO: ReadIO {
  fn read_freelist_overflow(
    &self, page_id: FreelistPageId, overflow: u32,
  ) -> crate::Result<Self::Bytes, DiskReadError>;

  fn read_node_overflow(
    &self, page_id: NodePageId, overflow: u32,
  ) -> crate::Result<Self::Bytes, DiskReadError>;
}
