mod common;

use bbolt_boyer_moore_magiclen::*;

const INPUT_DATA_PATH: &str = r"tests/data/utf8.txt";

#[test]
fn data_input_from_file() {
  common::data_input_from_file(
    INPUT_DATA_PATH,
    |text, pattern, answer, answer_not_full, answer_not_full_rev| {
      let bm = BMByte::from(pattern).unwrap();

      assert_eq!(answer, bm.find_full_all_in(text));
      assert_eq!(
        answer.iter().rev().copied().collect::<Vec<usize>>(),
        bm.rfind_full_all_in(text)
      );
      assert_eq!(answer_not_full, bm.find_all_in(text));
      assert_eq!(answer_not_full_rev, bm.rfind_all_in(text));
    },
  );
}
