use crate::common::buffer::PageBuffer;
use crate::common::ids::{FreePageId, FreelistPageId};
use crate::common::page::PageHeader;
use delegate::delegate;

pub struct FreelistPage<'tx> {
  page_id: FreelistPageId,
  start_index: u8,
  count: usize,
  page: PageBuffer<'tx>,
}

impl<'tx> FreelistPage<'tx> {
  pub fn new(page: PageBuffer) -> FreelistPage {
    let mut start_index = 0u8;
    let header = page.get_header();
    assert!(header.is_freelist());
    let page_id = header.get_page_id();
    let count: usize = {
      if header.count() == u16::MAX {
        start_index = 1;
        let count_slice = page.slice(size_of::<PageHeader>(), size_of::<u64>());
        let count: u64 = *bytemuck::from_bytes(count_slice);
        if count > u32::MAX as u64 {
          panic!("leading element count {} overflows u32", count);
        }
        count as usize
      } else {
        header.count() as usize
      }
    };
    let max_page_size = size_of::<PageHeader>()
      + (start_index as usize * size_of::<u64>())
      + (count * size_of::<u64>());
    assert!(page.len() >= max_page_size);
    FreelistPage {
      page_id,
      start_index,
      count,
      page,
    }
  }

  pub fn free_page_ids(&self) -> &[FreePageId] {
    let offset = size_of::<PageHeader>() + (self.start_index as usize * size_of::<u64>());
    let len = self.count * size_of::<u64>();
    let slice = self.page.slice(offset, len);
    bytemuck::cast_slice(slice)
  }

  delegate! {
    to self.page {
      pub fn get_header(&self) -> &PageHeader;
    }
  }
}

#[cfg(test)]
mod test {}
