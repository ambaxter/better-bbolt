use bon::__::IsSet;
use bon::{bon, builder, Builder};
use size::Size;
use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub struct InMemoryBolt {}

#[bon]
impl InMemoryBolt {
  #[builder]
  pub fn new(
    initial_size: Option<Size>, page_size: Option<usize>, max_batch_size: Option<u32>,
    max_batch_delay: Option<Duration>,
  ) -> io::Result<InMemoryBolt> {
    unimplemented!()
  }
}

pub struct FileBolt {}

#[bon]
impl FileBolt {
  #[builder(start_fn(name = "rw_builder"), finish_fn(name = "open"))]
  pub fn open<P: AsRef<Path>>(
    #[builder(finish_fn)] path: P, #[builder(default = false)] use_mlock: bool,
    initial_size: Option<Size>, page_size: Option<usize>, max_batch_size: Option<u32>,
    max_batch_delay: Option<Duration>,
  ) -> io::Result<FileBolt> {
    unimplemented!()
  }

  #[builder(start_fn(name = "ro_builder"), finish_fn(name = "open"))]
  pub fn open_ro<P: AsRef<Path>>(
    #[builder(finish_fn)] path: P, #[builder(default = false)] use_mlock: bool,
    initial_size: Option<Size>, page_size: Option<usize>, max_batch_size: Option<u32>,
    max_batch_delay: Option<Duration>,
  ) -> io::Result<FileBolt> {
    unimplemented!()
  }
}

#[cfg(test)]
mod tests {
  use crate::db::options::FileBolt;

  #[test]
  pub fn test() {}
}
