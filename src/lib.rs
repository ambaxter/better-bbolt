use std::cmp::Ordering;
use std::iter::FusedIterator;

pub mod bucket;
pub mod freelist;
pub mod tx;

pub mod db;

pub trait BBoltIterators: Iterator {
  fn overlays<J>(self, orig: J) -> OverlayIter<Self, J>
  where
    Self: Sized,
    J: Iterator<Item = Self::Item>,
  {
    OverlayIter {
      started: false,
      overlay_iter: self,
      overlay: None,
      orig_iter: orig,
      orig: None,
    }
  }
}

impl<I> BBoltIterators for I where I: Iterator {}

pub struct OverlayIter<I, R>
where
  I: Iterator,
  R: Iterator<Item = I::Item>,
{
  started: bool,
  overlay_iter: I,
  overlay: Option<I::Item>,
  orig_iter: R,
  orig: Option<I::Item>,
}

impl<I, R> Iterator for OverlayIter<I, R>
where
  I: Iterator,
  I::Item: PartialOrd + Copy,
  R: Iterator<Item = I::Item>,
{
  type Item = <I as Iterator>::Item;

  fn next(&mut self) -> Option<Self::Item> {
    if self.started {
      match (self.overlay, self.orig) {
        (Some(overlay), Some(orig)) => {
          if overlay == orig {
            self.overlay = self.overlay_iter.next();
            self.orig = self.orig_iter.next();
          } else if overlay < orig {
            self.overlay = self.overlay_iter.next();
          } else {
            self.orig = self.orig_iter.next();
          }
        }
        (None, Some(_)) => {
          self.orig = self.orig_iter.next();
        }
        (Some(_), None) => {
          self.overlay = self.overlay_iter.next();
        }
        (None, None) => return None,
      }
    } else {
      self.overlay = self.overlay_iter.next();
      self.orig = self.orig_iter.next();
    }
    self.started = true;
    match (self.overlay, self.orig) {
      (Some(overlay), Some(orig)) => {
        if overlay <= orig {
          Some(overlay)
        } else {
          Some(orig)
        }
      }
      (None, Some(orig)) => Some(orig),
      (Some(overlay), None) => Some(overlay),
      (None, None) => None,
    }
  }
}

impl<I, R> FusedIterator for OverlayIter<I, R>
where
  I: Iterator,
  I::Item: PartialOrd + Copy,
  R: Iterator<Item = I::Item>,
{
}

#[derive(Clone, Copy, Debug)]
pub enum ValueDelta<'a> {
  Disk(&'a str),
  Update(&'a str),
  Delete,
}

#[derive(Clone, Copy, Debug)]
pub struct TreeEntry<'a> {
  key: &'a str,
  value: ValueDelta<'a>,
}

impl<'a> PartialOrd for TreeEntry<'a> {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.key.cmp(other.key))
  }
}

impl<'a> PartialEq for TreeEntry<'a> {
  fn eq(&self, other: &Self) -> bool {
    self.key == other.key
  }
}

impl<'a> Eq for TreeEntry<'a> {}

impl<'a> TreeEntry<'a> {
  pub fn disk(key: &'a str, value: &'a str) -> TreeEntry<'a> {
    TreeEntry {
      key,
      value: ValueDelta::Disk(value),
    }
  }

  pub fn delta(key: &'a str, value: ValueDelta<'a>) -> TreeEntry<'a> {
    TreeEntry { key, value }
  }
}

#[cfg(test)]
mod tests {
  use crate::{BBoltIterators, TreeEntry, ValueDelta};
  use std::collections::BTreeMap;
  use std::fmt::Debug;

  #[test]
  fn test_btreemap() {
    let mut disk = BTreeMap::new();
    disk.insert("One", "Two");
    disk.insert("Three", "Four");
    disk.insert("Five", "Six");
    let mut delta = BTreeMap::new();
    delta.insert("One", ValueDelta::Delete);
    delta.insert("Five", ValueDelta::Update("Seven"));
    let disk = disk.iter().map(|(k, v)| TreeEntry::disk(k, v));
    let delta = delta.iter().map(|(k, v)| TreeEntry::delta(k, *v));
    let overlay = delta.overlays(disk);
    for TreeEntry { key, value } in overlay {
      println!("{}-{:?}", key, value);
    }
  }
}
