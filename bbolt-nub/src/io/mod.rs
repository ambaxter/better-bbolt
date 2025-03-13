use crate::common::errors::DiskReadError;
use crate::common::id::DiskPageId;
use crate::common::page_bytes::{Page, PageBytes};
use error_stack::Result;

pub trait ReadPageData {
  type ReadPageOut<'a>: PageBytes
  where
    Self: 'a;
  fn read_page_data<'a>(
    &'a self, disk_page_id: DiskPageId,
  ) -> Result<Self::ReadPageOut<'a>, DiskReadError>;
}

pub trait ReadContigPage: ReadPageData {
  fn read_contig_page<'a>(
    &'a self, disk_page_id: DiskPageId,
  ) -> Result<Page<Self::ReadPageOut<'a>>, DiskReadError>;
}

pub trait ReadNonContigPage: ReadPageData {
  type RootPageOut<'a>: PageBytes
  where
    Self: 'a;

  fn read_root_page<'a>(
    &'a self, disk_page_id: DiskPageId,
  ) -> Result<Page<Self::RootPageOut<'a>>, DiskReadError>;
}
