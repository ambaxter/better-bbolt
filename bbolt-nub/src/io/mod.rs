use crate::common::errors::DiskReadError;
use crate::common::id::DiskPageId;
use crate::common::page_bytes::{Page, PageBytes};
use error_stack::Result;

pub mod disk_cache;

//AsRef<[u8]>



pub trait ReadPageData {
  type RootDataBytes: PageBytes;

  type DataBytes: PageBytes;

  fn read_root_page(
    &self, disk_page_id: DiskPageId,
  ) -> Result<Page<Self::RootDataBytes>, DiskReadError>;

  fn read_page(&self, disk_page_id: DiskPageId) -> Result<Self::DataBytes, DiskReadError>;
}

pub trait ReadContigData: ReadPageData {}

pub trait ReadNonContigData: ReadPageData {}
