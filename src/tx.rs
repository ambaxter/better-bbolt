use bbolt_engine::backend::ReadHandle;
use std::marker::PhantomData;
use std::rc::Rc;

pub trait TxReadContext<'tx, R>
where
  R: ReadHandle<'tx>,
{
}

pub struct ReadContext<'tx, R>
where
  Self: 'tx,
{
  handle: R,
  bump: Rc<Bump>,
  _marker: PhantomData<&'tx ()>,
}

impl<'tx, R> ReadContext<'tx, R>
where
  R: ReadHandle<'tx>,
{
  pub fn new(handle: R, bump: Bump) -> Self {
    ReadContext {
      handle,
      bump: Rc::new(bump),
      _marker: PhantomData,
    }
  }

  pub fn alloc_slice<'a>(&'tx self, data: &'a [u8]) -> &'tx [u8] {
    self.bump.alloc_slice_copy(data)
  }
}

impl<'tx, R> TxReadContext<'tx, R> for ReadContext<'tx, R> where R: ReadHandle<'tx> {}
