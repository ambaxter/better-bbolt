use crate::common::buffer::OwnedBufferInner;
use aligners::AlignedBytes;
use crossbeam_channel::Sender;
use parking_lot::Mutex;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

pub enum PoolCommand {
  Push(Arc<OwnedBufferInner>),
  Exit,
}

pub struct BufferPool {
  pool: Arc<Mutex<Vec<Arc<OwnedBufferInner>>>>,
  gc_thread: JoinHandle<()>,
  gc_tx: Sender<PoolCommand>,
  page_size: usize,
}

impl BufferPool {
  pub fn new(page_size: usize, reset_page: bool) -> Arc<BufferPool> {
    let (gc_tx, gc_rx) = crossbeam_channel::unbounded();
    let pool = Arc::new(Mutex::new(Vec::new()));
    let gc_thread = {
      let pool = pool.clone();
      thread::spawn(move || {
        while let Ok(cmd) = gc_rx.recv() {
          match cmd {
            PoolCommand::Push(mut page) => {
              if reset_page {
                Arc::get_mut(&mut page).unwrap().reset();
              }
              pool.lock().push(page)
            }
            PoolCommand::Exit => break,
          }
        }
      })
    };
    Arc::new(BufferPool {
      pool,
      gc_thread,
      gc_tx,
      page_size,
    })
  }

  pub fn pop(&self) -> Arc<OwnedBufferInner> {
    let mut pool = self.pool.lock();
    pool.pop().unwrap_or_else(|| {
      let page = AlignedBytes::new_zeroed(self.page_size);
      OwnedBufferInner::new_with_tx(page, self.gc_tx.clone())
    })
  }
}

impl Drop for BufferPool {
  fn drop(&mut self) {
    self.gc_tx.send(PoolCommand::Exit).unwrap();
  }
}
