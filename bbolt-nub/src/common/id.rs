use crate::common::layout::page::PageFlag;
use bytemuck::{Pod, Zeroable};
use std::fmt::Debug;
use std::ops::{Add, Deref};
// TODO: Clean this up once I'm done making sure everything is as it needs to be!

pub trait DbId {
  fn of(id: u64) -> Self;
}

#[repr(C)]
#[derive(Default, Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Pod, Zeroable)]
pub struct DiskPageId(pub u64);

impl DbId for DiskPageId {
  #[inline]
  fn of(id: u64) -> Self {
    Self(id)
  }
}

#[repr(C)]
#[derive(Default, Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Pod, Zeroable)]
pub struct EOFPageId(pub DiskPageId);

impl Deref for EOFPageId {
  type Target = DiskPageId;
  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

#[repr(C)]
#[derive(Default, Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Pod, Zeroable)]
pub struct DbRsrcId(pub u64);

impl DbId for DbRsrcId {
  #[inline]
  fn of(id: u64) -> Self {
    Self(id)
  }
}

macro_rules! db_rsrc_id {
    (
    $(#[$meta:meta])*
    $x:ident
  ) => {
    $(#[$meta])*
    #[repr(C)]
    #[derive(Default, Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Pod, Zeroable)]
    pub struct $x(pub DbRsrcId);

    impl $x {
      #[inline]
      pub fn of(id: u64) -> Self {
        Self(DbRsrcId(id))
      }
    }
  }
}

db_rsrc_id!(TxId);

impl TxId {
  #[inline]
  pub fn meta_offset(self) -> u64 {
    self.0.0 % 2
  }
}

#[repr(C)]
#[derive(Default, Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Pod, Zeroable)]
pub struct DbPageId(pub u64);

pub trait DbPageType: Debug + Copy + Clone + Deref<Target = DbPageId> {
  fn page_type_mask(&self) -> PageFlag;
}

macro_rules! db_page_id {
    (
    $(#[$meta:meta])*
    $x:ident,$flag:stmt
  ) => {
    $(#[$meta])*
    #[repr(C)]
    #[derive(Default, Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Pod, Zeroable)]
    pub struct $x(pub DbPageId);

    impl $x {
      #[inline]
      fn of(id: u64) -> Self {
        Self(DbPageId(id))
      }
    }
      impl Deref for $x {
      type Target = DbPageId;

      #[inline]
      fn deref(&self) -> &Self::Target {
        &self.0
      }
    }

      impl DbPageType for $x {
      #[inline(always)]
        fn page_type_mask(&self) -> PageFlag {
          $flag
        }
    }
  }
}

db_page_id!(MetaPageId, PageFlag::META);
db_page_id!(BucketPageId, PageFlag::NODE_TYPE_MASK);
db_page_id!(NodePageId, PageFlag::NODE_TYPE_MASK);
db_page_id!(FreelistPageId, PageFlag::FREELIST);

impl From<BucketPageId> for NodePageId {
  #[inline]
  fn from(value: BucketPageId) -> Self {
    NodePageId(value.0)
  }
}

pub trait OverflowablePage: DbPageType + Add<u32> {}
macro_rules! overflowable_page_id {
  (
    $(#[$meta:meta])*
    $x:ident
  ) => {
    impl Add<u32> for $x {
      type Output = Self;

      #[inline]
      fn add(self, rhs: u32) -> Self {
        Self(DbPageId(self.0.0 + rhs as u64))
      }
    }

    impl OverflowablePage for $x {}
  };
}

overflowable_page_id!(NodePageId);
overflowable_page_id!(FreelistPageId);

#[derive(Debug, Copy, Clone)]
pub enum OverflowPageId {
  Freelist(FreelistPageId),
  Node(NodePageId),
}

pub trait DiskPageTranslator: Debug + Clone {
  fn meta(&self) -> DiskPageId;
  fn freelist(&self, page_id: FreelistPageId) -> DiskPageId;
  fn node(&self, page_id: NodePageId) -> DiskPageId;
}

pub trait SupportsContigPages {}
pub trait SupportsNonContigPages {}

#[derive(Debug, Clone)]
pub struct DirectPageTranslator {
  tx_id: TxId,
}

impl DirectPageTranslator {
  pub fn new(tx_id: TxId) -> Self {
    Self { tx_id }
  }
}

impl DiskPageTranslator for DirectPageTranslator {
  #[inline]
  fn meta(&self) -> DiskPageId {
    DiskPageId(self.tx_id.meta_offset())
  }

  #[inline]
  fn freelist(&self, page_id: FreelistPageId) -> DiskPageId {
    DiskPageId(page_id.0.0)
  }

  #[inline]
  fn node(&self, page_id: NodePageId) -> DiskPageId {
    DiskPageId(page_id.0.0)
  }
}

impl SupportsContigPages for DirectPageTranslator {}
impl SupportsNonContigPages for DirectPageTranslator {}

#[derive(Debug, Clone)]
pub struct StableFreeSpaceTranslator {
  tx_id: TxId,
  freespace_cluster_len: usize,
  page_size: usize,
}

impl StableFreeSpaceTranslator {
  pub fn new(tx_id: TxId, freespace_cluster_len: usize, page_size: usize) -> Self {
    StableFreeSpaceTranslator {
      tx_id,
      freespace_cluster_len,
      page_size,
    }
  }

  #[inline]
  pub fn cluster_len(&self) -> u64 {
    (2 * self.freespace_cluster_len as u64)
      + (self.freespace_cluster_len as u64 * self.page_size as u64 * 8)
  }
}

impl DiskPageTranslator for StableFreeSpaceTranslator {
  #[inline]
  fn meta(&self) -> DiskPageId {
    DiskPageId(self.tx_id.meta_offset())
  }

  fn freelist(&self, page_id: FreelistPageId) -> DiskPageId {
    let meta_offset = self.tx_id.meta_offset();
    let freespace_cluster_len = self.freespace_cluster_len as u64;
    let data_cluster_idx = page_id.0.0 / freespace_cluster_len;
    let freespace_offset = page_id.0.0 % freespace_cluster_len;
    let cluster_len = self.cluster_len();
    let disk_offset = 2 + // meta pages
      data_cluster_idx * cluster_len;
    let freespace_disk_id = disk_offset +
      (meta_offset * freespace_cluster_len) // meta defined freespace cluster
      + freespace_offset; // the offset within th  at cluster
    DiskPageId(freespace_disk_id)
  }

  fn node(&self, page_id: NodePageId) -> DiskPageId {
    let freespace_cluster_len = self.freespace_cluster_len as u64;
    let data_cluster_idx = page_id.0.0 / self.page_size as u64;
    let node_offset = page_id.0.0 % self.page_size as u64;
    let cluster_len = self.cluster_len();
    let disk_offset = 2 + // meta pages
      data_cluster_idx * cluster_len; // the data pages for each cluster;
    let node_disk_id = disk_offset + (2 * freespace_cluster_len) + node_offset;
    DiskPageId(node_disk_id)
  }
}

impl SupportsNonContigPages for StableFreeSpaceTranslator {}

#[cfg(test)]
mod tests {
  use crate::common::id::{
    DirectPageTranslator, DiskPageTranslator, FreelistPageId, NodePageId,
    StableFreeSpaceTranslator, TxId,
  };

  #[test]
  fn test() {
    let tr = DirectPageTranslator::new(TxId::of(1));
    let meta = tr.meta();
    let freelist_page = tr.freelist(FreelistPageId::of(2));
    let node_page = tr.node(NodePageId::of(3));
    println!("{:#?}", meta);
  }

  #[test]
  fn test2() {
    let tr = StableFreeSpaceTranslator::new(TxId::of(1), 2, 4096);
    let meta = tr.meta();
    let f0 = tr.freelist(FreelistPageId::of(0));
    let f1 = tr.freelist(FreelistPageId::of(1));
    let f2 = tr.freelist(FreelistPageId::of(2));
    let f3 = tr.freelist(FreelistPageId::of(3));
    let node_page = tr.node(NodePageId::of(0));
    println!("{:#?}", meta);
  }
}
