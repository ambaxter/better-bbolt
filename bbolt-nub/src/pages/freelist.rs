use crate::common::id::FreelistPageId;
use crate::pages::HasHeader;

pub trait HasFreelist : HasHeader {
  type FreelistIter: Iterator<Item = FreelistPageId>;

  fn freelist_iter(&self) -> Self::FreelistIter;
}