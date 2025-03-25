pub unsafe trait TxSliceIndex<'tx, T>
where
  T: ?Sized + 'tx,
{
  type Output: 'tx;

  fn get(&self, slice: &T) -> Option<Self::Output>;
  unsafe fn get_unchecked(&self, slice: &T) -> Option<Self::Output>;
}
