use bytemuck::{Pod, Zeroable};
use std::fmt::{Display, Formatter};
use std::ops::{Add, AddAssign, Div, Sub, SubAssign};

macro_rules! id {

  (
    $(#[$meta:meta])*
    $x:ident
  ) => {

    $(#[$meta])*
    #[repr(C)]
    #[derive(Default, Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Pod, Zeroable)]
    pub struct $x(pub u64);

    impl Display for $x {
      fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
      }
    }

    impl From<u64> for $x {
      #[inline(always)]
      fn from(value: u64) -> Self {
        $x(value)
      }
    }

    impl From<$x> for u64 {
      #[inline(always)]
      fn from(value: $x) -> Self {
        value.0
      }
    }

    impl Add<u64> for $x {
      type Output = $x;

      #[inline(always)]
      fn add(self, rhs: u64) -> Self::Output {
        $x(self.0 + rhs)
      }
    }

    impl Sub<u64> for $x {
      type Output = $x;

      #[inline(always)]
      fn sub(self, rhs: u64) -> Self::Output {
        $x(self.0 - rhs)
      }
    }

    impl AddAssign<u64> for $x {
      #[inline(always)]
      fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs;
      }
    }

    impl SubAssign<u64> for $x {
      #[inline(always)]
      fn sub_assign(&mut self, rhs: u64) {
        self.0 -= rhs;
      }
    }


    impl Add<$x> for $x {
      type Output = $x;

      #[inline(always)]
      fn add(self, rhs: $x) -> Self::Output {
        $x(self.0 + rhs.0)
      }
    }

    impl Sub<$x> for $x {
      type Output = $x;

      #[inline(always)]
      fn sub(self, rhs: $x) -> Self::Output {
        $x(self.0 - rhs.0)
      }
    }

    impl AddAssign<$x> for $x {
      #[inline(always)]
      fn add_assign(&mut self, rhs: $x) {
        self.0 += rhs.0;
      }
    }

    impl SubAssign<$x> for $x {
      #[inline(always)]
      fn sub_assign(&mut self, rhs: $x) {
        self.0 -= rhs.0;
      }
    }

    impl Div<u64> for $x {
      type Output = $x;
      #[inline(always)]
      fn div(self, rhs: u64) -> Self::Output {
        $x(self.0 / rhs)
      }
    }

    impl PartialEq<$x> for u64 {
      #[inline(always)]
      fn eq(&self, other: &$x) -> bool {
        *self == other.0
      }
    }

  };
}

id!(
  /// The Page ID. Page address = `PgId` * page_size
  PageId
);

impl PageId {
  /// Create a PgId
  #[inline(always)]
  pub const fn of(id: u64) -> PageId {
    PageId(id)
  }
}

id!(
  /// The Transaction ID. Monotonic and incremented every commit
  TxId
);

impl TxId {
  /// Create a TxId
  #[inline(always)]
  pub const fn of(id: u64) -> TxId {
    TxId(id)
  }
}

pub trait GetPageId {
  fn page_id(&self) -> PageId;
}

impl<T> From<T> for PageId
where
  T: GetPageId,
{
  #[inline(always)]
  fn from(id: T) -> Self {
    id.page_id()
  }
}

macro_rules! page_id {

  (
    $(#[$meta:meta])*
    $x:ident
  ) => {

    $(#[$meta])*
    #[repr(C)]
    #[derive(Default, Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Pod, Zeroable)]
    pub struct $x{
      page_id: PageId,
    }

    impl $x {

      #[inline(always)]
      pub const fn new(page_id: PageId) -> Self {
        Self{page_id}
      }
    }

    impl Display for $x {
      fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.page_id)
      }
    }

    impl GetPageId for $x {

      #[inline(always)]
       fn page_id(&self) -> PageId {
        self.page_id
      }
    }

    impl From<PageId> for $x {
      fn from(id: PageId) -> Self {
        Self::new(id)
      }
    }

  };
}

page_id!(
  /// The end of the database where EOF = meta.eof_id * meta.page_size
  EOFPageId
);

impl EOFPageId {
  /// Create an EOF Page Id
  #[inline(always)]
  pub const fn of(id: u64) -> EOFPageId {
    assert!(id > 1);
    EOFPageId::new(PageId::of(id))
  }
}

page_id!(
  /// The Node Page id. Page address = `PgId` * page_size
  NodePageId
);

impl NodePageId {
  /// Create a NodePageId
  #[inline(always)]
  pub const fn of(id: u64) -> NodePageId {
    assert!(id > 1);
    NodePageId::new(PageId::of(id))
  }
}

page_id!(
  /// The Free ID. Page address = `PgId` * page_size
  FreePageId
);

impl FreePageId {
  /// Create a FreePgId
  #[inline(always)]
  pub const fn of(id: u64) -> FreePageId {
    assert!(id > 1);
    FreePageId::new(PageId::of(id))
  }
}

page_id!(
  /// The Bucket Page ID
  BucketPageId
);

impl BucketPageId {
  /// Create a Bucket Page ID
  #[inline(always)]
  pub const fn of(id: u64) -> BucketPageId {
    assert!(id > 1);
    BucketPageId::new(PageId::of(id))
  }

  #[inline(always)]
  pub const fn inline_page() -> BucketPageId {
    BucketPageId::new(PageId::of(0))
  }
}

page_id!(
  /// The Freelist Page ID
  FreelistPageId
);

impl FreelistPageId {
  /// Create a Freelist Page ID
  #[inline(always)]
  pub const fn of(id: u64) -> FreelistPageId {
    assert!(id > 1);
    FreelistPageId::new(PageId::of(id))
  }

  #[inline(always)]
  pub const fn no_freelist() -> FreelistPageId {
    FreelistPageId::of(0xffffffffffffffff)
  }
}

page_id!(
  /// The Meta ID. Either Page 0 or 1
  MetaPageId
);

impl MetaPageId {
  /// Create a MetaId
  #[inline(always)]
  pub const fn of(id: u64) -> MetaPageId {
    assert!(id <= 1);
    MetaPageId::new(PageId::of(id))
  }

  #[inline(always)]
  pub const fn zero() -> MetaPageId {
    MetaPageId::of(0)
  }

  #[inline(always)]
  pub const fn one() -> MetaPageId {
    MetaPageId::of(1)
  }
}

page_id!(
  /// The Disk Page Id
  DiskPageId
);

impl DiskPageId {
  /// Create a DiskPageId
  #[inline(always)]
  pub const fn of(id: u64) -> DiskPageId {
    DiskPageId::new(PageId::of(id))
  }
}

page_id!(
  /// The Page Id for new territory
  ExtendedPageId
);

impl ExtendedPageId {
  /// Create an ExtendedPageId
  #[inline(always)]
  pub const fn of(id: u64) -> ExtendedPageId {
    assert!(id > 1);
    ExtendedPageId::new(PageId::of(id))
  }
}

macro_rules! id_transition {
  (
    $from:ident,$to:ident
  ) => {
    impl From<$from> for $to {
      #[inline(always)]
      fn from(from: $from) -> Self {
        Self {
          page_id: from.page_id,
        }
      }
    }

    impl From<$to> for $from {
      #[inline(always)]
      fn from(from: $to) -> Self {
        Self {
          page_id: from.page_id,
        }
      }
    }
  };
}

id_transition!(FreelistPageId, FreePageId);
id_transition!(NodePageId, FreePageId);
id_transition!(BucketPageId, FreePageId);

id_transition!(ExtendedPageId, FreelistPageId);
id_transition!(ExtendedPageId, NodePageId);
id_transition!(ExtendedPageId, BucketPageId);

impl From<BucketPageId> for NodePageId {
  fn from(value: BucketPageId) -> Self {
    assert_ne!(0, value.page_id.0);
    NodePageId::new(PageId::of(value.page_id.0))
  }
}

/// PageId / 8
#[derive(Default, Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct LotIndex(pub usize);

/// PageId % 8
#[derive(Default, Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct LotOffset(pub u8);

impl PageId {
  #[inline(always)]
  pub fn lot_index_and_offset(self) -> (LotIndex, LotOffset) {
    let page_id = self.0 as usize;
    (LotIndex(page_id / 8), LotOffset((page_id % 8) as u8))
  }
}

impl LotIndex {
  #[inline(always)]
  pub fn abs_diff(self, other: LotIndex) -> usize {
    self.0.abs_diff(other.0)
  }
}
