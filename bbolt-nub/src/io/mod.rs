use parking_lot::{RwLock, RwLockReadGuard};
use crate::common::buffer_pool::SharedBuffer;
use crate::common::errors::DiskReadError;
use crate::common::id::{DiskPageId, FreelistPageId, MetaPageId, NodePageId};
use crate::io::reader::BaseReader;
use crate::pages::Page;
use crate::pages::bytes::TxPage;
use crate::pages::freelist::FreelistPage;
use crate::pages::meta::MetaPage;

pub mod disk_cache;
pub mod memmap;
pub mod reader;

pub trait ReadPage<'tx>: Sized {
  type PageData: TxPage<'tx>;

  fn read_page(&self, disk_page_id: DiskPageId) -> crate::Result<Self::PageData, DiskReadError>;
}

pub trait ReadContigPage<'tx>: ReadPage<'tx> {
  fn read_contig_page(&self, disk_page_id: DiskPageId) -> crate::Result<Self::PageData, DiskReadError>;
}

pub struct PageReaderWrap<'tx, T, R> {
  reader: RwLockReadGuard<'tx, R>,
  translator: T
}

impl<'tx, T, R> PageReaderWrap<'tx, T, R> where R: ReadPage<'tx> {
  fn read(&self) -> crate::Result<R::PageData, DiskReadError> {
    self.reader.read_page(DiskPageId(0))
  }
}

#[derive(Debug, Copy, Clone)]
struct DummyReader;

impl<'tx> ReadPage<'tx> for DummyReader {
  type PageData = SharedBuffer;

  fn read_page(&self, disk_page_id: DiskPageId) -> error_stack::Result<Self::PageData, DiskReadError> {
    todo!()
  }
}

pub struct BaseWrapper<R: for<'tx> ReadPage<'tx> >{
  f: RwLock<R>
}

impl<R: for<'tx> ReadPage<'tx>> BaseWrapper<R> {
  fn fork(&self) -> PageReaderWrap<u64, R> {
    PageReaderWrap {
      reader: self.f.read(),
      translator: 6u64,
    }
  }
}

fn t(){
  let t = BaseWrapper::<DummyReader>{f: RwLock::new(DummyReader)};
  let m = t.fork();
  assert_eq!(true, m.read().is_ok())
}

pub trait ReadData<'tx>: Sized {
  type PageData: TxPage<'tx>;

  fn read_disk(
    &self, disk_page_id: DiskPageId, pages: usize,
  ) -> crate::Result<Self::PageData, DiskReadError>;
}

pub trait ContigReader<'tx>: ReadData<'tx> {
  type PageType: TxPage<'tx>;

  fn read_meta(
    &self, meta_page_id: MetaPageId,
  ) -> crate::Result<MetaPage<Self::PageType>, DiskReadError>;

  fn read_freelist(
    &self, freelist_page_id: FreelistPageId,
  ) -> crate::Result<FreelistPage<Self::PageType>, DiskReadError>;

  fn read_node(
    &self, node_page_id: NodePageId,
  ) -> crate::Result<Page<Self::PageType>, DiskReadError>;
}

pub trait NonContigReader<'tx>: ContigReader<'tx> {
  fn read_freelist_overflow(
    &self, root_page_id: FreelistPageId, overflow: u32,
  ) -> crate::Result<Self::PageData, DiskReadError>;
  fn read_node_overflow(
    &self, root_page_id: NodePageId, overflow: u32,
  ) -> crate::Result<Self::PageData, DiskReadError>;
}
