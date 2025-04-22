use std::cmp::Ordering;
use std::cmp::Ordering::{Equal, Greater, Less};
use std::hint;

pub trait TrySliceExt<T> {
  fn try_binary_search_by<'a, F, E>(&'a self, f: F) -> crate::Result<Result<usize, usize>, E>
  where
    F: FnMut(&'a T) -> crate::Result<Ordering, E>,
    T: 'a;
}

impl<T> TrySliceExt<T> for [T] {
  fn try_binary_search_by<'a, F, E>(&'a self, mut f: F) -> crate::Result<Result<usize, usize>, E>
  where
    F: FnMut(&'a T) -> crate::Result<Ordering, E>,
    T: 'a,
  {
    let mut size = self.len();
    if size == 0 {
      return Ok(Err(0));
    }
    let mut base = 0usize;

    // This loop intentionally doesn't have an early exit if the comparison
    // returns Equal. We want the number of loop iterations to depend *only*
    // on the size of the input slice so that the CPU can reliably predict
    // the loop count.
    while size > 1 {
      let half = size / 2;
      let mid = base + half;

      // SAFETY: the call is made safe by the following inconstants:
      // - `mid >= 0`: by definition
      // - `mid < size`: `mid = size / 2 + size / 4 + size / 8 ...`
      let cmp = f(unsafe { self.get_unchecked(mid) })?;

      // Binary search interacts poorly with branch prediction, so force
      // the compiler to use conditional moves if supported by the target
      // architecture.
      // TODO: select_unpredictable is unstable so I can't use it here, yet
      // Hopefully, soooooon!
      // https://github.com/rust-lang/rust/issues/133962
      //base = (cmp == Greater).select_unpredictable(base, mid);
      base = if cmp == Greater { base } else { mid };

      // This is imprecise in the case where `size` is odd and the
      // comparison returns Greater: the mid element still gets included
      // by `size` even though it's known to be larger than the element
      // being searched for.
      //
      // This is fine though: we gain more performance by keeping the
      // loop iteration count invariant (and thus predictable) than we
      // lose from considering one additional element.
      size -= half;
    }

    // SAFETY: base is always in [0, size) because base <= mid.
    let cmp = f(unsafe { self.get_unchecked(base) })?;
    if cmp == Equal {
      // SAFETY: same as the `get_unchecked` above.
      unsafe { hint::assert_unchecked(base < self.len()) };
      Ok(Ok(base))
    } else {
      let result = base + (cmp == Less) as usize;
      // SAFETY: same as the `get_unchecked` above.
      // Note that this is `<=`, unlike the assume in the `Ok` path.
      unsafe { hint::assert_unchecked(result <= self.len()) };
      Ok(Err(result))
    }
  }
}
