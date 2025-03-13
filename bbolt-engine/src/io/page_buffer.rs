use crate::io::read_buffer::ReadBuffer;

pub enum PageBuffer<'tx> {
  Direct(ReadBuffer),
  Extended(ReadBuffer, &'tx [u8]),
  Mapped(&'tx [u8]),
  NonContiguous,
}
