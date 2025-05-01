use crossbeam_channel::{Receiver, RecvError, Sender, TryRecvError, TrySendError};
use std::borrow::Borrow;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct ChannelEntry<T> {
  t: Option<T>,
  sender: Sender<T>,
  outstanding: Arc<AtomicUsize>,
}

impl<T> Deref for ChannelEntry<T> {
  type Target = T;
  fn deref(&self) -> &Self::Target {
    self.t.as_ref().unwrap()
  }
}

impl<T> DerefMut for ChannelEntry<T> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    self.t.as_mut().unwrap()
  }
}

impl<T> Borrow<T> for ChannelEntry<T> {
  fn borrow(&self) -> &T {
    &self.t
  }
}

impl<T> Drop for ChannelEntry<T> {
  fn drop(&mut self) {
    if let Some(t) = self.t.take() {
      self.outstanding.fetch_sub(1, Ordering::Release);
      self.sender.send(t).ok();
    }
  }
}

pub struct ChannelStore<T> {
  sender: Sender<T>,
  receiver: Receiver<T>,
  outstanding: Arc<AtomicUsize>,
}

impl<T> ChannelStore<T>
where
  T: Send + Sync + 'static,
{
  pub fn new_with_capacity(capacity: usize) -> Self {
    let (sender, receiver) = crossbeam_channel::bounded(capacity);
    let outstanding = Arc::new(AtomicUsize::new(0));
    ChannelStore {
      sender,
      receiver,
      outstanding,
    }
  }

  pub fn new() -> Self {
    let (sender, receiver) = crossbeam_channel::unbounded();
    let outstanding = Arc::new(AtomicUsize::new(0));
    ChannelStore {
      sender,
      receiver,
      outstanding,
    }
  }

  pub fn len(&self) -> usize {
    self.sender.len()
  }

  pub fn outstanding(&self) -> usize {
    self.outstanding.load(Ordering::Acquire)
  }

  pub fn pop(&self) -> crate::Result<ChannelEntry<T>, RecvError> {
    let t = self.receiver.recv()?;
    self.outstanding.fetch_add(1, Ordering::Release);
    Ok(ChannelEntry {
      t: Some(t),
      sender: self.sender.clone(),
      outstanding: self.outstanding.clone(),
    })
  }

  pub fn clear(&self) -> crate::Result<(), TryRecvError> {
    loop {
      let r = self.receiver.try_recv();
      match r {
        Ok(_) => {}
        Err(TryRecvError::Empty) => return Ok(()),
        Err(TryRecvError::Disconnected) => return Err(TryRecvError::Disconnected.into()),
      }
    }
  }

  pub fn push(&self, entry: T) -> crate::Result<(), TrySendError<T>> {
    self.sender.try_send(entry)?;
    Ok(())
  }

  pub fn extend<I: IntoIterator<Item = T>>(
    &self, entries: I,
  ) -> crate::Result<(), TrySendError<T>> {
    for entry in entries {
      self.sender.try_send(entry)?;
    }
    Ok(())
  }
}
