use crate::common::errors::IOError;
use crate::common::id::DiskPageId;
use crate::common::layout::meta::HeaderMetaPage;
use bytemuck::bytes_of_mut;
use error_stack::ResultExt;
use std::fs::File;
use std::io;
use std::io::{BufReader, Read, Seek, SeekFrom};

pub struct MetaReader {
  reader: BufReader<File>,
  file_size: u64,
}

impl MetaReader {
  pub fn new(reader: BufReader<File>) -> Self {
    let file_size = reader.get_ref().metadata().unwrap().len();
    MetaReader { reader, file_size }
  }

  fn first_metadata(&mut self) -> io::Result<Option<HeaderMetaPage>> {
    let mut meta_page = HeaderMetaPage::default();
    self.reader.seek(SeekFrom::Start(0))?;
    self.reader.read_exact(bytes_of_mut(&mut meta_page))?;
    self
      .reader
      .seek_relative(-(size_of::<HeaderMetaPage>() as i64))?;
    if meta_page.meta.is_valid() {
      Ok(Some(meta_page))
    } else {
      Ok(None)
    }
  }

  fn second_metadata(&mut self) -> io::Result<Option<HeaderMetaPage>> {
    let mut meta_page = HeaderMetaPage::default();
    let mut current_pos = 0;
    for i in 0..15u64 {
      let meta_pos = 1024u64 << i;
      if self.file_size < 1024 || meta_pos >= self.file_size - 1024 {
        break;
      }
      self.reader.seek_relative((meta_pos - current_pos) as i64)?;
      self.reader.read_exact(bytes_of_mut(&mut meta_page))?;
      if meta_page.meta.is_valid() {
        return Ok(Some(meta_page));
      }
      self
        .reader
        .seek_relative(-(size_of::<HeaderMetaPage>() as i64))?;
      current_pos = meta_pos;
    }
    Ok(None)
  }

  fn check_metadata(&mut self) -> io::Result<Option<HeaderMetaPage>> {
    match (self.first_metadata()?, self.second_metadata()?) {
      (Some(meta_page), Some(second_metadata)) => {
        if meta_page.meta.tx_id > second_metadata.meta.tx_id {
          Ok(Some(meta_page))
        } else {
          Ok(Some(second_metadata))
        }
      }
      (Some(meta_page), None) => Ok(Some(meta_page)),
      (None, Some(second_metadata)) => Ok(Some(second_metadata)),
      (None, None) => Ok(None),
    }
  }

  pub fn determine_file_meta(mut self) -> crate::Result<HeaderMetaPage, IOError> {
    let meta_page = self.check_metadata().change_context(IOError::MetaError)?;
    match meta_page {
      None => Err(IOError::MetaError.into()),
      Some(meta) => Ok(meta),
    }
  }
}

#[cfg(test)]
mod tests {
  use crate::io::backends::meta_reader::MetaReader;
  use std::fs::File;
  use std::io::BufReader;

  #[test]
  fn test_meta_reader() {
    let file = File::open("my.db").unwrap();
    let mut meta_reader = MetaReader::new(BufReader::new(file));
    let meta_page = meta_reader.determine_file_meta().unwrap();
    println!("{:?}", meta_page);
  }
}
