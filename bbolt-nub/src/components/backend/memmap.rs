use crate::common::errors::DbError;
use bon::{bon, builder};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub struct MemmapOptions {
  //MMap Specific
  preload_freelist: bool,
  use_mlock: bool,
  // Common
  disable_growth_sync: bool,
  disable_freelist_sync: bool,
}

pub struct MemmapDb {}

#[bon]
impl MemmapDb {
  #[builder(finish_fn = open_path)]
  pub fn new(
    #[builder(finish_fn)]
    #[builder(into)]
    path: PathBuf, page_size: Option<usize>,
    file_lock_timeout: Option<Duration>, #[builder(default)] use_mlock: bool,
  ) -> crate::Result<Self, DbError> {
    todo!()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  #[test]
  fn test() {
    let b = MemmapDb::builder().open_path("Test");
  }
}
