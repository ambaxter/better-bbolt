use crate::backend::ReadHandle;
use crate::common::bucket::BucketBuffer;
use crate::cursor::Cursor;

pub struct BucketIndex<'tx> {
  root_page: BucketBuffer<'tx>,
}

impl<'tx> BucketIndex<'tx> {
  pub fn new(root_page: BucketBuffer<'tx>) -> BucketIndex<'tx> {
    BucketIndex { root_page }
  }

  pub fn cursor<R>(&self, read_handle: R) -> Cursor<'tx, R>
  where
    R: ReadHandle<'tx>,
  {
    let root_page_id = self.root_page.get_header().id().into();
    Cursor::new(root_page_id, read_handle)
  }
}
