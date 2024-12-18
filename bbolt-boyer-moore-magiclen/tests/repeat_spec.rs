use bbolt_boyer_moore_magiclen::repeat_spec::{BMRepeat, FindEnds};
use serde::{Deserialize, Serialize};
use std::io::Read;

const INPUT_DATA_PATH: &str = r"tests/data/repeat_spec.ron";

#[derive(Debug, Copy, Clone, Deserialize, Serialize)]
pub enum ByteMask {
  Either(u8, u8),
  Both(u8, u8),
}

impl ByteMask {
  fn find_ends<'a>(&'a self) -> FindEnds<impl Fn(u8, u8) -> Option<(u8, u8)> + 'a> {
    match self {
      ByteMask::Either(mask_l, mask_r) => FindEnds::Either(*mask_l, *mask_r),
      ByteMask::Both(mask_l, mask_r) => FindEnds::Both(|l, r| {
        if (*mask_l & l == *mask_l) && (*mask_r & r == *mask_r) {
          Some((*mask_l, *mask_r))
        } else {
          None
        }
      }),
    }
  }
}
#[derive(Debug, Deserialize, Serialize)]
pub struct MaskTest {
  rev: bool,
  mask: ByteMask,
  repeat_len: usize,
  haystack: Vec<u8>,
  expected_index: Option<usize>,
}

#[test]
fn data_input_from_file() {
  let mut file = std::fs::File::open(INPUT_DATA_PATH).unwrap();
  let mut contents = String::new();
  file.read_to_string(&mut contents).unwrap();
  let tests: Vec<MaskTest> = ron::from_str(&*contents).unwrap();
  for (idx, test) in tests.iter().enumerate() {
    let bm = BMRepeat::new(255, test.repeat_len);
    let r = if test.rev {
      bm.rfind_first_in(&test.haystack, test.mask.find_ends())
    } else {
      bm.find_first_in(&test.haystack, test.mask.find_ends())
    };
    match (test.expected_index, r) {
      (Some(expected_index), Some(result)) => assert_eq!(
        expected_index, result.index,
        "Index: {idx} - Expected index {expected_index}, but found {}",
        result.index
      ),
      (Some(expected_index), None) => {
        panic!("Index: {idx} - Expected index {expected_index}, but found None")
      }
      (None, Some(result)) => panic!(
        "Index: {idx} - Expected no index, but found Some({})",
        result.index
      ),
      _ => {}
    }
  }
}
