use crate::common::buffer_pool::SharedBuffer;

pub trait PageBytes: Clone + AsRef<[u8]> {}

impl<'a> PageBytes for &'a [u8] {}
impl PageBytes for SharedBuffer {}
