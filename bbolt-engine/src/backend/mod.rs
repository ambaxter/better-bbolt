use crate::backend::memory::MemoryBackend;
use crate::common::bucket::{BucketBuffer, BucketHeader};
use crate::common::buffer::PageBuffer;
use crate::common::ids::{
  BucketPageId, EOFPageId, FreelistPageId, GetPageId, MetaPageId, NodePageId, PageId, TxId,
};
use crate::common::page::PageHeader;
use crate::index::BucketIndex;
use crate::pages::freelist::FreelistPage;
use crate::pages::meta::{Meta, MetaPage};
use crate::pages::node::{BranchElement, BranchPage, LeafElement, LeafFlag, NodePage, NodeType};
use aligners::{AlignedBytes, alignment};
use bytemuck::bytes_of;
use itertools::izip;
use parking_lot::{RwLock, RwLockReadGuard, RwLockUpgradableReadGuard};
use size::Size;
use size::consts::MiB;
use std::io;
use std::rc::Rc;
use std::time::Duration;

pub mod file;
pub mod memory;
pub mod mmap;

pub mod closed;

pub mod shared;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DbIdentifier {
  pub version: u32,
  pub magic: u32,
}

pub const BBOLT_COMPATIBLE_ID: DbIdentifier = DbIdentifier {
  version: 2,
  magic: 0xED0CDAED,
};

pub const ORIG_BBOLT_RS_ID: DbIdentifier = DbIdentifier {
  version: 2,
  // Chosen from https://nedbatchelder.com/text/hexwords.html
  // as we are using the Go BBolt project code as a scaffold
  magic: 0x5caff01d,
};

pub const BETTER_BBOLT_RS_ID: DbIdentifier = DbIdentifier {
  // Lucky Number 7!
  version: 777,
  // Chosen from https://nedbatchelder.com/text/hexwords.html
  // as we are using the Go BBolt project code as a scaffold
  magic: 0x5caff01d,
};

pub const IGNORE_NO_SYNC: bool = cfg!(target_os = "openbsd");

pub const DEFAULT_MAX_BATCH_SIZE: u32 = 1000;
pub const DEFAULT_MAX_BATCH_DELAY: Duration = Duration::from_millis(10);
pub const DEFAULT_ALLOC_SIZE: Size = Size::from_const(16 * MiB);

pub fn initialize_database(page_size: Size) -> AlignedBytes<alignment::Page> {
  let page_size = page_size.bytes() as usize;
  let mut buffer = AlignedBytes::new_zeroed(4 * page_size);
  for (i, bytes) in buffer.chunks_mut(page_size).enumerate() {
    match i {
      0..2 => {
        let page_header = PageHeader::init_meta(PageId::of(i as u64));
        let meta = Meta::init(page_size, BBOLT_COMPATIBLE_ID, TxId::of(i as u64));
        MetaPage::write(bytes, &page_header, &meta);
      }
      2 => {
        let page_header = PageHeader::init_freelist(PageId::of(i as u64));
        PageHeader::write(bytes, &page_header);
      }
      3 => {
        let page_header = PageHeader::init_leaf(PageId::of(i as u64));
        PageHeader::write(bytes, &page_header);
      }
      _ => break,
    }
  }
  buffer
}

pub fn initialize_dummy_database(page_size: Size) -> AlignedBytes<alignment::Page> {
  let page_size = page_size.bytes() as usize;
  let mut buffer = AlignedBytes::new_zeroed(19 * page_size);
  for (i, bytes) in buffer.chunks_mut(page_size).enumerate() {
    match i {
      0 => {
        let page_header = PageHeader::init_meta(PageId::of(i as u64));
        let meta = Meta::init(page_size, BBOLT_COMPATIBLE_ID, TxId::of(i as u64));
        MetaPage::write(bytes, &page_header, &meta);
      }
      1 => {
        let page_header = PageHeader::init_meta(PageId::of(i as u64));
        let mut meta = Meta::init(page_size, BBOLT_COMPATIBLE_ID, TxId::of(i as u64));
        meta.eof_id = EOFPageId::of(20);
        meta.root = BucketHeader::new(BucketPageId::of(4), 0);
        meta.update_checksum();
        MetaPage::write(bytes, &page_header, &meta);
      }
      2 => {
        let mut page_header = PageHeader::init_freelist(PageId::of(i as u64));
        page_header.set_count(13);
        PageHeader::write(bytes, &page_header);
        let freelist = &mut bytes[size_of::<PageHeader>()
          ..size_of::<PageHeader>() + (page_header.count() as usize * size_of::<u64>())];
        let freelist_buffer: &mut [u64] = bytemuck::cast_slice_mut(freelist);
        freelist_buffer
          .iter_mut()
          .enumerate()
          .for_each(|(a, v)| *v = a as u64);
      }
      3 => {
        let page_header = PageHeader::init_leaf(PageId::of(i as u64));
        PageHeader::write(bytes, &page_header);
      }
      4 => {
        let mut page_header = PageHeader::init_leaf(PageId::of(i as u64));
        page_header.set_count(3);
        let mut elements = [LeafElement::new_bucket(); 3];
        let keys = ["a_bucket", "b_bucketofthing", "c_bucket_typeof_thing"];
        let values = [
          {
            let inline_bucket = BucketHeader::new(BucketPageId::inline_page(), 0);
            let mut inline_page = PageHeader::init_leaf(PageId::of(0));
            inline_page.set_count(4);
            let mut elements = [LeafElement::new(); 4];
            let keys = ["a", "about", "alot", "apple"];
            let values = [
              aligned_from_str("string 0"),
              aligned_from_str("string 1"),
              aligned_from_str("string 2"),
              aligned_from_str("string 3"),
            ];

            let inline_buffer_len = size_of::<PageHeader>()
              + (size_of::<LeafElement>() * 4)
              + izip!(&keys, &values)
                .map(|(key, value)| key.as_bytes().len() + value.len())
                .sum::<usize>();

            let mut inline_buffer = AlignedBytes::<alignment::Page>::new_zeroed(
              size_of::<BucketHeader>() + inline_buffer_len,
            );
            let (bh, ip) = inline_buffer.split_at_mut(size_of::<BucketHeader>());
            bh.copy_from_slice(bytes_of(&inline_bucket));
            write_leaf_page(ip, &inline_page, &mut elements, &keys, &values);
            inline_buffer
          },
          {
            let inline_bucket = BucketHeader::new(BucketPageId::of(5), 0);
            let mut buffer = AlignedBytes::<alignment::Page>::new_zeroed(size_of::<BucketHeader>());
            buffer.copy_from_slice(bytes_of(&inline_bucket));
            buffer
          },
          {
            let inline_bucket = BucketHeader::new(BucketPageId::of(6), 0);
            let mut buffer = AlignedBytes::<alignment::Page>::new_zeroed(size_of::<BucketHeader>());
            buffer.copy_from_slice(bytes_of(&inline_bucket));
            buffer
          },
        ];
        write_leaf_page(bytes, &page_header, &mut elements, &keys, &values);
      }
      5 => {
        let mut page_header = PageHeader::init_branch(PageId::of(i as u64));
        page_header.set_count(4);
        let mut elements = [
          BranchElement::new_with_page(NodePageId::of(7)),
          BranchElement::new_with_page(NodePageId::of(8)),
          BranchElement::new_with_page(NodePageId::of(9)),
          BranchElement::new_with_page(NodePageId::of(10)),
        ];
        let keys = ["a", "b", "c", "d"];
        write_branch_page(bytes, &page_header, &mut elements, &keys);
      }
      6 => {
        let mut page_header = PageHeader::init_branch(PageId::of(i as u64));
        page_header.set_count(4);
        let mut elements = [
          BranchElement::new_with_page(NodePageId::of(11)),
          BranchElement::new_with_page(NodePageId::of(12)),
          BranchElement::new_with_page(NodePageId::of(13)),
          BranchElement::new_with_page(NodePageId::of(14)),
        ];
        let keys = ["e", "f", "g", "h"];
        write_branch_page(bytes, &page_header, &mut elements, &keys);
      }
      7 => {
        let mut page_header = PageHeader::init_leaf(PageId::of(i as u64));
        page_header.set_count(4);
        let mut elements = [LeafElement::new(); 4];
        let keys = ["a", "about", "alot", "apple"];
        let values = [
          aligned_from_str("string 0"),
          aligned_from_str("string 1"),
          aligned_from_str("string 2"),
          aligned_from_str("string 3"),
        ];
        write_leaf_page(bytes, &page_header, &mut elements, &keys, &values);
      }
      8 => {
        let mut page_header = PageHeader::init_leaf(PageId::of(i as u64));
        page_header.set_count(4);
        let mut elements = [LeafElement::new(); 4];
        let keys = ["b", "bat", "bear", "bottle"];
        let values = [
          aligned_from_str("string 0"),
          aligned_from_str("string 1"),
          aligned_from_str("string 2"),
          aligned_from_str("string 3"),
        ];
        write_leaf_page(bytes, &page_header, &mut elements, &keys, &values);
      }
      9 => {
        let mut page_header = PageHeader::init_leaf(PageId::of(i as u64));
        page_header.set_count(4);
        let mut elements = [LeafElement::new(); 4];
        let keys = ["c", "cat", "care bear", "cold"];
        let values = [
          aligned_from_str("string 0"),
          aligned_from_str("string 1"),
          aligned_from_str("string 2"),
          aligned_from_str("string 3"),
        ];
        write_leaf_page(bytes, &page_header, &mut elements, &keys, &values);
      }
      10 => {
        let mut page_header = PageHeader::init_leaf(PageId::of(i as u64));
        page_header.set_count(4);
        let mut elements = [LeafElement::new(); 4];
        let keys = ["d", "dart", "dealt", "dread"];
        let values = [
          aligned_from_str("string 0"),
          aligned_from_str("string 1"),
          aligned_from_str("string 2"),
          aligned_from_str("string 3"),
        ];
        write_leaf_page(bytes, &page_header, &mut elements, &keys, &values);
      }
      11 => {
        let mut page_header = PageHeader::init_leaf(PageId::of(i as u64));
        page_header.set_count(4);
        let mut elements = [LeafElement::new(); 4];
        let keys = ["e", "ear", "edible", "exist"];
        let values = [
          aligned_from_str("string 0"),
          aligned_from_str("string 1"),
          aligned_from_str("string 2"),
          aligned_from_str("string 3"),
        ];
        write_leaf_page(bytes, &page_header, &mut elements, &keys, &values);
      }
      12 => {
        let mut page_header = PageHeader::init_leaf(PageId::of(i as u64));
        page_header.set_count(4);
        let mut elements = [LeafElement::new(); 4];
        let keys = ["f", "fart", "flammable", "fort"];
        let values = [
          aligned_from_str("string 0"),
          aligned_from_str("string 1"),
          aligned_from_str("string 2"),
          aligned_from_str("string 3"),
        ];
        write_leaf_page(bytes, &page_header, &mut elements, &keys, &values);
      }
      13 => {
        let mut page_header = PageHeader::init_leaf(PageId::of(i as u64));
        page_header.set_count(4);
        let mut elements = [LeafElement::new(); 4];
        let keys = ["g", "garble", "gentry", "gorb"];
        let values = [
          aligned_from_str("string 0"),
          aligned_from_str("string 1"),
          aligned_from_str("string 2"),
          aligned_from_str("string 3"),
        ];
        write_leaf_page(bytes, &page_header, &mut elements, &keys, &values);
      }
      13 => {
        let mut page_header = PageHeader::init_leaf(PageId::of(i as u64));
        page_header.set_count(4);
        let mut elements = [LeafElement::new(); 4];
        let keys = ["h", "heart", "holler", "hunt"];
        let values = [
          aligned_from_str("string 0"),
          aligned_from_str("string 1"),
          aligned_from_str("string 2"),
          aligned_from_str("string 3"),
        ];
        write_leaf_page(bytes, &page_header, &mut elements, &keys, &values);
      }
      19 => {
        let page_header = PageHeader::init_leaf(PageId::of(i as u64));
        PageHeader::write(bytes, &page_header);
      }
      _ => break,
    }
  }
  buffer
}

pub fn aligned_from_str(s: &str) -> AlignedBytes<alignment::Page> {
  let mut buffer = AlignedBytes::new_zeroed(s.as_bytes().len());
  buffer.copy_from_slice(s.as_bytes());
  buffer
}

pub fn write_branch_page(
  buffer: &mut [u8], page_header: &PageHeader, elements: &mut [BranchElement], keys: &[&str],
) {
  let elem_start = size_of::<PageHeader>();
  PageHeader::write(buffer, page_header);
  let leaf_node = &mut buffer[elem_start..];
  let mut key_start = size_of::<LeafElement>() * elements.len();
  for (idx, (elem, key)) in izip!(elements, keys).enumerate() {
    let key_bytes = key.as_bytes();
    let elem_start = size_of::<LeafElement>() * idx;
    let elem_buffer = &mut leaf_node[elem_start..elem_start + size_of::<LeafElement>()];
    elem.set_key_dist((key_start - elem_start) as u32);
    elem.set_key_len(key_bytes.len() as u32);
    elem_buffer.copy_from_slice(bytes_of(elem));
    let key_buffer = &mut leaf_node[key_start..key_start + key_bytes.len()];
    key_buffer.copy_from_slice(key_bytes);
    key_start += key_bytes.len();
  }
}

pub fn write_leaf_page(
  buffer: &mut [u8], page_header: &PageHeader, elements: &mut [LeafElement], keys: &[&str],
  values: &[AlignedBytes<alignment::Page>],
) {
  let elem_start = size_of::<PageHeader>();
  PageHeader::write(buffer, page_header);
  let leaf_node = &mut buffer[elem_start..];
  let mut key_start = size_of::<LeafElement>() * elements.len();
  for (idx, (elem, key, value)) in izip!(elements, keys, values).enumerate() {
    let key_bytes = key.as_bytes();
    let elem_start = size_of::<LeafElement>() * idx;
    let elem_buffer = &mut leaf_node[elem_start..elem_start + size_of::<LeafElement>()];
    elem.set_key_dist((key_start - elem_start) as u32);
    elem.set_key_len(key_bytes.len() as u32);
    elem.set_value_len(value.len() as u32);
    elem_buffer.copy_from_slice(bytes_of(elem));
    let key_buffer = &mut leaf_node[key_start..key_start + key_bytes.len()];
    key_buffer.copy_from_slice(key_bytes);
    let value_start = key_start + key_bytes.len();
    let value_buffer = &mut leaf_node[value_start..value_start + value.len()];
    value_buffer.copy_from_slice(value);
    key_start += key_bytes.len() + value.len();
  }
}

pub struct PagingSystem<T> {
  backend: RwLock<T>,
}

impl<T> PagingSystem<T>
where
  T: PagingBackend,
{
  pub fn new(backend: T) -> Self {
    Self {
      backend: RwLock::new(backend),
    }
  }

  pub fn read_handle(&self) -> T::RHandle<'_> {
    T::read_handle(self.backend.read())
  }

  pub fn write_handle(&self) -> T::RWHandle<'_> {
    T::write_handle(self.backend.upgradable_read())
  }
}

pub trait PagingBackend: Sync + Send {
  type RHandle<'a>: ReadHandle<'a>
  where
    Self: 'a;
  type RWHandle<'a>: WriteHandle<'a>
  where
    Self: 'a;

  fn read_handle<'a>(lock: RwLockReadGuard<'a, Self>) -> Self::RHandle<'a>;
  fn write_handle<'a>(lock: RwLockUpgradableReadGuard<'a, Self>) -> Self::RWHandle<'a>;
}

pub trait ReadHandle<'p> {
  fn page_in(&self, page_id: PageId) -> io::Result<PageBuffer<'p>>;

  fn read_meta(&self, page_id: MetaPageId) -> io::Result<MetaPage<'p>> {
    self.page_in(page_id.page_id()).map(MetaPage::new)
  }

  fn read_freelist(&self, page_id: FreelistPageId) -> io::Result<FreelistPage<'p>> {
    self.page_in(page_id.page_id()).map(FreelistPage::new)
  }

  fn read_node(&self, page_id: NodePageId) -> io::Result<NodePage<'p>> {
    self.page_in(page_id.page_id()).map(NodePage::new)
  }
}

pub trait WriteHandle<'a>: ReadHandle<'a> {
  fn write<T: Into<Vec<u8>>>(&mut self, pages: Vec<(PageId, T)>);
}

#[cfg(test)]
mod test {
  use crate::backend::initialize_dummy_database;
  use crate::common::bucket::BucketHeader;
  use crate::common::buffer::PageBuffer;
  use crate::pages::freelist::FreelistPage;
  use crate::pages::meta::MetaPage;
  use crate::pages::node::{LeafPage, LeafValue, NodePage, NodeType};
  use size::Size;

  #[test]
  pub fn meta() {
    let page_size = page_size::get();
    let mm = initialize_dummy_database(Size::from_bytes(page_size::get()));
    let page_offset = 1 * page_size;
    let meta_buffer = &mm[page_offset..page_offset + page_size];
    let meta0_page = MetaPage::new(PageBuffer::Mapped(meta_buffer));
    let meta = meta0_page.get_meta();
    let header = meta0_page.get_header();
    let h = header.flags();
  }

  #[test]
  pub fn freelist() {
    let page_size = page_size::get();
    let mm = initialize_dummy_database(Size::from_bytes(page_size::get()));
    let page_offset = 2 * page_size;
    let freelist_buffer = &mm[page_offset..page_offset + page_size];
    let freelist_page = FreelistPage::new(PageBuffer::Mapped(freelist_buffer));
    let header = freelist_page.get_header();
    let freelist = freelist_page.free_page_ids();
    let h = header.flags();
  }

  #[test]
  pub fn empty_leaf() {
    let page_size = page_size::get();
    let mm = initialize_dummy_database(Size::from_bytes(page_size::get()));
    let page_offset = 3 * page_size;
    let leafpage_buffer = &mm[page_offset..page_offset + page_size];
    let node_page = NodePage::new(PageBuffer::Mapped(leafpage_buffer));
    let header = node_page.get_header();
    let leaf_page = node_page.access();
    match leaf_page {
      NodeType::Branch(_) => unreachable!(),
      NodeType::Leaf(leaf) => {
        let b = leaf.search(b"anything");
        let bb = b.is_none();
      }
    }
    let h = header.flags();
  }

  #[test]
  pub fn leaf_buckets() {
    let page_size = page_size::get();
    let mm = initialize_dummy_database(Size::from_bytes(page_size::get()));
    let page_offset = 4 * page_size;
    let leafpage_buffer = &mm[page_offset..page_offset + page_size];
    let node_page = NodePage::new(PageBuffer::Mapped(leafpage_buffer));
    let header = node_page.get_header();
    let leaf_page = node_page.access();
    match leaf_page {
      NodeType::Branch(_) => unreachable!(),
      NodeType::Leaf(leaf) => {
        if let Some(LeafValue::Bucket(bytes)) = leaf
          .search(b"a_bucket")
          .map(|idx| leaf.get_value(idx))
          .flatten()
        {
          let is_aligned = bytes.as_ptr().cast::<BucketHeader>().is_aligned();
          let bucket_buffer = &bytes[0..size_of::<BucketHeader>()];
          let bucket: BucketHeader = if is_aligned {
            *bytemuck::from_bytes(bucket_buffer)
          } else {
            bytemuck::pod_read_unaligned(bucket_buffer)
          };
          let l = bytes.len();
        }
        if let Some(LeafValue::Bucket(bytes)) = leaf
          .search(b"b_bucketofthing")
          .map(|idx| leaf.get_value(idx))
          .flatten()
        {
          let is_aligned = bytes.as_ptr().cast::<BucketHeader>().is_aligned();
          let bucket_buffer = &bytes[0..size_of::<BucketHeader>()];
          let bucket: BucketHeader = if is_aligned {
            *bytemuck::from_bytes(bucket_buffer)
          } else {
            bytemuck::pod_read_unaligned(bucket_buffer)
          };
          let l = bytes.len();
        }
        if let Some(LeafValue::Bucket(bytes)) = leaf
          .search(b"c_bucket_typeof_thing")
          .map(|idx| leaf.get_value(idx))
          .flatten()
        {
          let is_aligned = bytes.as_ptr().cast::<BucketHeader>().is_aligned();
          let bucket_buffer = &bytes[0..size_of::<BucketHeader>()];
          let bucket: BucketHeader = if is_aligned {
            *bytemuck::from_bytes(bucket_buffer)
          } else {
            bytemuck::pod_read_unaligned(bucket_buffer)
          };
          let l = bytes.len();
        }
      }
    }
    let h = header.flags();
  }
}

#[test]
pub fn branch() {
  let page_size = page_size::get();
  let mm = initialize_dummy_database(Size::from_bytes(page_size::get()));
  let page_offset = 5 * page_size;
  let branchpage_buffer = &mm[page_offset..page_offset + page_size];
  let node_page = NodePage::new(PageBuffer::Mapped(branchpage_buffer));
  let header = node_page.get_header();
  let branch = node_page.access();
  match branch {
    NodeType::Branch(branch) => {
      let a = branch.search(b"a");
      let b = branch.search(b"b");
      let c = branch.search(b"c");
      let d = branch.search(b"d");
      println!("{:?}", d)
    }
    NodeType::Leaf(_) => unreachable!(),
  }
  let h = header.flags();
}

#[test]
pub fn cursor() -> io::Result<()> {
  let page_size = page_size::get();
  let mm = initialize_dummy_database(Size::from_bytes(page_size::get()));
  let mem = PagingSystem::new(MemoryBackend::new(page_size, mm));
  let r = mem.read_handle();
  let meta0 = r.read_meta(MetaPageId::zero())?;
  let m0c = r.read_node(meta0.get_meta().root.root().into())?;
  let index = BucketIndex::new(BucketBuffer::Owned(m0c));
  let mut cursor = index.cursor(r.clone());
  let first = cursor.first()?;
  let meta1 = r.read_meta(MetaPageId::one())?;
  let m1c = r.read_node(meta1.get_meta().root.root().into())?;
  let index = BucketIndex::new(BucketBuffer::Owned(m1c));
  let mut cursor = index.cursor(r.clone());
  let first = cursor.first()?;
  let next = cursor.next()?;
  Ok(())
}
