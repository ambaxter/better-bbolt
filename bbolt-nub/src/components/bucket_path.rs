use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::iter::FusedIterator;

#[derive(Clone)]
pub struct BucketPathBuf {
  keys: Vec<u8>,
  partitions: Vec<usize>,
}

impl BucketPathBuf {
  pub fn new() -> BucketPathBuf {
    BucketPathBuf {
      keys: vec![],
      partitions: vec![],
    }
  }
  pub fn root<P: AsRef<[u8]>>(root: P) -> BucketPathBuf {
    let mut keys = Vec::new();
    let mut partitions = Vec::new();
    let root = root.as_ref();
    keys.extend_from_slice(root);
    partitions.push(root.len());
    BucketPathBuf { keys, partitions }
  }

  pub fn pop(&mut self) -> bool {
    self.partitions.pop();
    if let Some(last) = self.partitions.last() {
      self.keys.truncate(*last);
      true
    } else {
      self.keys.clear();
      false
    }
  }

  pub fn push<P: AsRef<[u8]>>(&mut self, key: P) {
    let key = key.as_ref();
    let partition = if let Some(last) = self.partitions.last() {
      *last + key.len()
    } else {
      key.len()
    };
    self.keys.extend_from_slice(key);
    self.partitions.push(partition);
  }

  pub fn len(&self) -> usize {
    self.partitions.len()
  }
}

impl<A> Extend<A> for BucketPathBuf
where
  A: AsRef<[u8]>,
{
  fn extend<T: IntoIterator<Item = A>>(&mut self, iter: T) {
    for item in iter.into_iter() {
      self.push(item.as_ref());
    }
  }
}

impl<A> FromIterator<A> for BucketPathBuf
where
  A: AsRef<[u8]>,
{
  fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
    let mut path = BucketPathBuf::new();
    path.extend(iter);
    path
  }
}

#[derive(Clone)]
pub struct BucketPathIterator<'a> {
  path: &'a BucketPathBuf,
  idx: usize,
  started: bool,
}

impl<'a> BucketPathIterator<'a> {
  pub fn new(path: &'a BucketPathBuf) -> BucketPathIterator<'a> {
    BucketPathIterator {
      path,
      idx: 0,
      started: false,
    }
  }
}

impl<'a> Iterator for BucketPathIterator<'a> {
  type Item = &'a [u8];
  fn next(&mut self) -> Option<Self::Item> {
    if !self.started {
      self.started = true;
      self.idx = 0;
      if let Some(partition) = self.path.partitions.get(self.idx) {
        Some(&self.path.keys[0..*partition])
      } else {
        None
      }
    } else {
      self.idx += 1;
      match (
        self.path.partitions.get(self.idx - 1),
        self.path.partitions.get(self.idx),
      ) {
        (Some(begin), Some(end)) => Some(&self.path.keys[*begin..*end]),
        _ => None,
      }
    }
  }
}

impl<'a> IntoIterator for &'a BucketPathBuf {
  type Item = &'a [u8];
  type IntoIter = BucketPathIterator<'a>;

  fn into_iter(self) -> Self::IntoIter {
    BucketPathIterator::new(self)
  }
}

impl<'a> ExactSizeIterator for BucketPathIterator<'a> {
  fn len(&self) -> usize {
    self.path.len()
  }
}

impl<'a> FusedIterator for BucketPathIterator<'a> {}

impl Debug for BucketPathBuf {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    f.write_str("BucketPathBuf {")?;
    f.debug_list().entries(self.into_iter()).finish()?;
    f.write_str("}")
  }
}

impl Display for BucketPathBuf {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    f.write_str("BucketPathBuf {[")?;
    for path in self.into_iter() {
      f.write_str("[")?;
      f.write_str(&String::from_utf8_lossy(path))?;
      f.write_str("],")?;
    }
    f.write_str("]}")
  }
}

impl PartialEq for BucketPathBuf {
  fn eq(&self, other: &Self) -> bool {
    self.partitions == other.partitions && self.keys == other.keys
  }
}

impl Eq for BucketPathBuf {}

impl PartialOrd for BucketPathBuf {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for BucketPathBuf {
  fn cmp(&self, other: &Self) -> Ordering {
    match self.partitions.cmp(&other.partitions) {
      Ordering::Less => Ordering::Less,
      Ordering::Equal => self.into_iter().cmp(other.into_iter()),
      Ordering::Greater => Ordering::Greater,
    }
  }
}

pub struct BucketPathBuilder {
  path: BucketPathBuf,
}

impl Hash for BucketPathBuf {
  fn hash<H: Hasher>(&self, state: &mut H) {
    state.write_usize(self.len());
    for path in self.into_iter() {
      state.write(path);
    }
  }
}

impl BucketPathBuilder {
  pub fn new() -> BucketPathBuilder {
    BucketPathBuilder {
      path: BucketPathBuf::new(),
    }
  }

  pub fn root<P: AsRef<[u8]>>(path: P) -> BucketPathBuilder {
    BucketPathBuilder {
      path: BucketPathBuf::root(path),
    }
  }

  pub fn push<P: AsRef<[u8]>>(mut self, path: P) -> Self {
    self.path.push(path);
    self
  }

  pub fn finish(self) -> BucketPathBuf {
    self.path
  }
}

impl<T> From<T> for BucketPathBuf
where
  T: IntoIterator,
  T::Item: AsRef<[u8]>,
{
  fn from(value: T) -> Self {
    Self::from_iter(value)
  }
}

fn test_into<T: Into<BucketPathBuf>>(val: T) {
  println!("{}", val.into());
}

fn test_discriminant<const M: usize, const N: usize>(
  read: [BucketPathBuf; M], write: [BucketPathBuf; N],
) -> ([Option<BucketPathBuf>; M], [Option<BucketPathBuf>; N]) {
  (read.map(|t| Some(t)), write.map(|w| Some(w)))
}

fn test_discriminant_ref<'a>(
  read: &'a [BucketPathBuf], write: &'a [BucketPathBuf],
) -> (Vec<Option<BucketPathBuf>>, Vec<Option<BucketPathBuf>>) {
  (
    read.into_iter().cloned().map(|v| Some(v)).collect(),
    write.into_iter().cloned().map(|v| Some(v)).collect(),
  )
}

fn test_discriminant_iter<'a, T, U>(
  read: T, write: U,
) -> (Vec<Option<BucketPathBuf>>, Vec<Option<BucketPathBuf>>)
where
  T: IntoIterator<Item = BucketPathBuf>,
  U: IntoIterator<Item = BucketPathBuf>,
{
  let read = read.into_iter().map(|v| Some(v)).collect();
  let write = write.into_iter().map(|v| Some(v)).collect();
  (read, write)
}

#[derive(Debug, Copy, Clone)]
pub struct NoBuckets;

impl From<NoBuckets> for BucketPathBuf {
  fn from(value: NoBuckets) -> Self {
    unreachable!()
  }
}

impl<'a> From<&'a NoBuckets> for BucketPathBuf {
  fn from(value: &'a NoBuckets) -> Self {
    unreachable!()
  }
}

#[cfg(test)]
mod tests {
  use super::{
    BucketPathBuf, BucketPathBuilder, BucketPathIterator, NoBuckets, test_discriminant,
    test_discriminant_iter, test_discriminant_ref, test_into,
  };

  #[test]
  fn test() {
    let mut bucket1 = BucketPathBuilder::root("root")
      .push("bucket")
      .push("foo")
      .push("bar")
      .finish();
    test_into(bucket1);
    let mut bucket2: BucketPathBuf = ["root2", "bucket", "foo", "bar"].into();
    test_into(bucket2);
  }

  #[test]
  fn test_disc() {
    let (read, write) = test_discriminant(
      [],
      [["root", "next2"].into(), ["root2", "next3", "next4"].into()],
    );
    println!("read: {:?}", read);
    println!("write: {:?}", write);
  }

  #[test]
  fn test_disc_ref() {
    let (read, write) =
      test_discriminant_ref(&[], &[["root", "next2"].into(), ["root2", "next3"].into()]);
    println!("read: {:?}", read);
    println!("write: {:?}", write);
  }

  #[test]
  fn test_disc_iter() {
    let (read, write) =
      test_discriminant_iter([], [["root", "next2"].into(), ["root2", "next3"].into()]);
    println!("read: {:?}", read);
    println!("write: {:?}", write);
  }
}
