use crate::common::page::PageFlag;
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
  fn of(id: u64) -> Self {
    Self(id)
  }
}

#[repr(C)]
#[derive(Default, Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Pod, Zeroable)]
pub struct EOFPageId(pub DiskPageId);

impl Deref for EOFPageId {
  type Target = DiskPageId;
  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

#[repr(C)]
#[derive(Default, Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Pod, Zeroable)]
pub struct DbRsrcId(pub u64);

impl DbId for DbRsrcId {
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

  }
}

db_rsrc_id!(TxId);

#[repr(C)]
#[derive(Default, Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Pod, Zeroable)]
pub struct DbPageId(u64);

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

      impl Deref for $x {
      type Target = DbPageId;
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

pub trait OverflowablePage: DbPageType + Add<u32> {}
macro_rules! overflowable_page_id {
  (
    $(#[$meta:meta])*
    $x:ident
  ) => {
    impl Add<u32> for $x {
      type Output = Self;
      fn add(self, rhs: u32) -> Self {
        Self(DbPageId(self.0.0 + rhs as u64))
      }
    }

    impl OverflowablePage for $x {}
  };
}

overflowable_page_id!(BucketPageId);
overflowable_page_id!(NodePageId);
overflowable_page_id!(FreelistPageId);

pub trait TranslatablePage: DbPageType {}

impl TranslatablePage for BucketPageId {}
impl TranslatablePage for NodePageId {}
impl TranslatablePage for FreelistPageId {}
