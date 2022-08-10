use std::{cmp::max, collections::HashMap};

fn inverted(mut vec: Vec<i32>) -> Vec<i32> {
    for v in &mut vec {
        if *v == 0 {
            *v = 1;
        } else {
            *v = 0;
        }
    }
    vec
}

fn max_equal_rows_after_flips(matrix: Vec<Vec<i32>>) -> i32 {
    let mut counts: HashMap<Vec<i32>, i32> = HashMap::new();
    for vec in matrix {
        let entry = counts.entry(vec.clone()).or_insert(0);
        *entry += 1;
        let entry = counts.entry(inverted(vec)).or_insert(0);
        *entry += 1;
    }
    let mut max_cnt = 0;
    for (_, cnt) in counts {
        max_cnt = max(max_cnt, cnt);
    }
    max_cnt
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test1() {
        let matrix: Vec<Vec<i32>> = vec![vec![0, 1], vec![1, 1]];
        let res = max_equal_rows_after_flips(matrix);
        let expected = 1;
        assert_eq!(res, expected)
    }

    #[test]
    fn test2() {
        let matrix: Vec<Vec<i32>> = vec![vec![0, 1], vec![1, 0]];
        let res = max_equal_rows_after_flips(matrix);
        let expected = 2;
        assert_eq!(res, expected)
    }

    #[test]
    fn test3() {
        let matrix: Vec<Vec<i32>> = vec![vec![0, 0, 0], vec![0, 0, 1], vec![1, 1, 0]];
        let res = max_equal_rows_after_flips(matrix);
        let expected = 2;
        assert_eq!(res, expected)
    }
}

fn main() {
    println!("Hello, world!");
}
