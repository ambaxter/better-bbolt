use crate::common::bucket::BucketHeader;
use crate::common::page::PageHeader;
use aligners::alignment::Alignment;

#[derive(Debug)]
pub struct PageHeaderAlignment {}

unsafe impl Alignment for PageHeaderAlignment {
  fn size() -> usize {
    align_of::<PageHeader>()
  }
}

#[derive(Debug)]
pub struct BucketHeaderAlignment {}

unsafe impl Alignment for BucketHeaderAlignment {
  fn size() -> usize {
    align_of::<BucketHeader>()
  }
}
