use crate::backend::ReadHandle;
use crate::common::ids::NodePageId;
use crate::pages::node::{BranchPage, LeafPage, LeafValue, NodePage, NodeType};
use std::io;

pub struct StackEntry<'a> {
  node_page: NodePage<'a>,
  index: usize,
}

impl<'a> StackEntry<'a> {
  pub fn new(node_page: NodePage<'a>) -> Self {
    Self {
      node_page,
      index: 0,
    }
  }

  pub fn new_with_index(node_page: NodePage<'a>, index: usize) -> Self {
    Self { node_page, index }
  }

  #[inline]
  pub fn is_leaf(&self) -> bool {
    self.node_page.is_leaf()
  }

  #[inline]
  pub fn get_count(&self) -> usize {
    self.node_page.get_count()
  }

  #[inline]
  pub fn get_page_id(&self) -> NodePageId {
    self.node_page.get_header().get_page_id()
  }

  pub fn get_branch(&self) -> BranchPage {
    match self.node_page.access() {
      NodeType::Branch(branch) => branch,
      NodeType::Leaf(_) => panic!(
        "tried to get leaf from branch page {}",
        self.node_page.get_header().id()
      ),
    }
  }

  pub fn get_leaf(&self) -> LeafPage {
    match self.node_page.access() {
      NodeType::Leaf(leaf) => leaf,
      NodeType::Branch(_) => panic!(
        "tried to get branch from leaf page {}",
        self.node_page.get_header().id()
      ),
    }
  }
}

pub struct Cursor<'p, R> {
  read_handle: R,
  root: NodePageId,
  stack: Vec<StackEntry<'p>>,
}

impl<'p, R> Cursor<'p, R>
where
  R: ReadHandle<'p>,
{
  pub fn new(root: NodePageId, read_handle: R) -> Self {
    Cursor {
      read_handle,
      root,
      stack: vec![],
    }
  }

  pub fn first(&mut self) -> io::Result<Option<(&[u8], LeafValue)>> {
    self.stack.clear();
    let node = self.read_handle.read_node(self.root)?;
    self.stack.push(StackEntry::new(node));

    self.go_to_first_element_on_stack()?;

    if self.stack.last().unwrap().get_count() == 0 {
      self.next()?;
    }

    Ok(self.key_value())
  }

  pub fn last(&mut self) -> io::Result<Option<(&[u8], LeafValue)>> {
    self.stack.clear();
    let node = self.read_handle.read_node(self.root)?;
    let last_node = node.get_count() - 1;
    self.stack.push(StackEntry::new_with_index(node, last_node));
    self.go_to_last_element_on_stack()?;

    while self.stack.len() > 1 && self.stack.last().unwrap().get_count() == 0 {
      self.prev()?;
    }

    if self.stack.is_empty() {
      return Ok(None);
    }

    Ok(self.key_value())
  }

  pub fn next(&mut self) -> io::Result<Option<(&[u8], LeafValue)>> {
    loop {
      // Attempt to move over one element until we're successful.
      // Move up the stack as we hit the end of each page in our stack.
      let mut stack_exhausted = true;
      let mut new_stack_depth = 0;
      for (depth, entry) in self.stack.iter_mut().enumerate().rev() {
        new_stack_depth = depth + 1;
        if (entry.index as i32) < entry.get_count() as i32 - 1 {
          entry.index += 1;
          stack_exhausted = false;
          break;
        }
      }

      if stack_exhausted {
        return Ok(None);
      }
      self.stack.truncate(new_stack_depth);
      self.go_to_first_element_on_stack()?;

      if let Some(entry) = self.stack.last_mut() {
        if entry.get_count() == 0 {
          continue;
        }
      }
      return Ok(self.key_value());
    }
  }

  pub fn prev(&mut self) -> io::Result<Option<(&[u8], LeafValue)>> {
    loop {
      // Attempt to move over one element until we're successful.
      // Move up the stack as we hit the end of each page in our stack.
      let mut new_stack_depth = 0;
      let mut stack_exhausted = true;
      for (depth, entry) in self.stack.iter_mut().enumerate().rev() {
        new_stack_depth = depth + 1;
        if entry.index > 0 {
          entry.index -= 1;
          stack_exhausted = false;
          break;
        }
      }

      // If we've hit the beginning, we should stop moving the cursor,
      // and stay at the first element, so that users can continue to
      // iterate over the elements in reverse direction by calling `Next`.
      // We should return nil in such case.
      // Refer to https://github.com/etcd-io/bbolt/issues/733
      if new_stack_depth == 1 {
        self.first()?;
        return Ok(None);
      }

      if stack_exhausted {
        self.stack.clear();
      } else {
        self.stack.truncate(new_stack_depth);
      }

      // If we've hit the end then return None
      if self.stack.is_empty() {
        return Ok(None);
      }

      self.stack.truncate(new_stack_depth);

      // Move down the stack to find the last element of the last leaf under this branch.
      self.go_to_last_element_on_stack()?;

      return Ok(self.key_value());
    }
  }

  pub fn key_value(&mut self) -> Option<(&[u8], LeafValue)> {
    let entry = self.stack.last().unwrap();
    let count = entry.get_count();

    if count == 0 || entry.index > count {
      return None;
    }

    let leaf = entry.get_leaf();
    leaf.get_kv(entry.index)
  }

  fn go_to_first_element_on_stack(&mut self) -> io::Result<()> {
    loop {
      let node_id = {
        let entry = self
          .stack
          .last()
          .expect("go_to_first_element_on_stack: stack empty");
        if entry.node_page.is_leaf() {
          break;
        }
        let branch = entry.get_branch();
        branch
          .get(entry.index)
          .expect("go_to_first_element_on_stack: branch empty")
      };
      let node = self.read_handle.read_node(node_id)?;
      self.stack.push(StackEntry::new(node));
    }
    Ok(())
  }

  fn go_to_last_element_on_stack(&mut self) -> io::Result<()> {
    loop {
      let node_id = {
        let entry = self
          .stack
          .last()
          .expect("go_to_last_element_on_stack: stack empty");
        if entry.node_page.is_leaf() {
          break;
        }
        let branch = entry.get_branch();
        branch
          .get(entry.index)
          .expect("go_to_last_element_on_stack: branch empty")
      };
      let node = self.read_handle.read_node(node_id)?;
      let last_node = node.get_count() - 1;

      self.stack.push(StackEntry::new_with_index(node, last_node));
    }
    Ok(())
  }

  pub fn seek(&mut self, key: &[u8]) -> io::Result<Option<(&[u8], LeafValue)>> {
    self.stack.clear();
    let root = self.root;
    self.search(key, root)?;

    if let Some(entry) = self.stack.last() {
      if entry.index >= entry.get_count() {
        return Ok(self.next()?);
      }
    }

    Ok(self.key_value())
  }

  fn search(&mut self, key: &[u8], page_id: NodePageId) -> io::Result<()> {
    let node = self.read_handle.read_node(page_id)?;
    let node_is_leaf = node.get_header().is_leaf();

    self.stack.push(StackEntry::new(node));

    if node_is_leaf {
      self.search_leaf(key);
      return Ok(());
    }

    self.search_branch(key)
  }

  fn search_branch(&mut self, key: &[u8]) -> io::Result<()> {
    let page_id = {
      let entry = self.stack.last_mut().unwrap();
      let branch = entry.get_branch();
      let index = branch.search(key);
      let page_id = branch.get(index).unwrap();
      entry.index = index;
      page_id
    };
    self.search(key, page_id)
  }

  // TODO: How do we do this with rayon?
  // Switch away from partition_point?
  fn search_leaf(&mut self, key: &[u8]) {
    if let Some(entry) = self.stack.last_mut() {
      let leaf = entry.get_leaf();
      entry.index = leaf.partition_point(key);
    }
  }
}
