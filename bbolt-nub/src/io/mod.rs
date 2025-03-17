use crate::common::errors::DiskReadError;
use crate::common::id::DiskPageId;
use crate::pages::{Page, PageBytes};
use error_stack::Result;

pub mod disk_cache;

//AsRef<[u8]>

pub trait ReadPageData {
  type DataBytes: PageBytes;
  fn read_page(&self, disk_page_id: DiskPageId) -> Result<Self::DataBytes, DiskReadError>;
}

pub trait ReadContigData: ReadPageData {}

pub trait ReadNonContigData: ReadPageData {}
