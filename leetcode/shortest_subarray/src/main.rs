use rand::Rng;

fn calc_prefix_sums(nums: &mut Vec<i32>) -> Vec<i64> {
    let mut result: Vec<i64> = Vec::with_capacity(nums.len());
    let mut sum = 0i64;
    for i in 0..nums.len() {
        sum += nums[i] as i64;
        result.push(sum);
    }
    result
}

fn shortest_subarray(nums: Vec<i32>, k: i32) -> i32 {
    let mut nums = nums;
    if nums[0] >= k {
        return 1;
    }
    // calculate prefix sums
    let prefix_sums = calc_prefix_sums(&mut nums);
    let mut monotonic: Vec<(i64, i64)> = Vec::with_capacity(prefix_sums.len()); // value, idx
    monotonic.push((0, 0));
    let mut shortest: i64 = i64::MAX;
    let mut idx = 0; // current position in the "monotonic"
    for i in 0..prefix_sums.len() {
        let current = prefix_sums[i];
        // iterate over current monotonic array, checking prefix, trying to move threshold to the right
        while idx < monotonic.len() {
            if current - monotonic[idx].0 >= k as i64 {
                // found possible match
                let length = 1 + i as i64 - monotonic[idx].1;
                if length < shortest {
                    shortest = length;
                }
            } else {
                break; // idx now points to the leftmost point in "monotonic" for which subarray sum is < k
            }
            idx += 1;
        }
        // insert current element to the monotonic
        while let Some(last) = monotonic.last() {
            if last.0 > current {
                monotonic.pop();
            } else {
                break;
            }
        }
        monotonic.push((current, (i + 1) as i64));
    }
    if shortest == i64::MAX {
        shortest = -1;
    }
    shortest as i32
}

fn shortest_subarray_naive(nums: Vec<i32>, k: i32) -> i32 {
    let mut shortest = i32::MAX;
    for i in 0..nums.len() {
        let mut sum = 0;
        for j in i..nums.len() {
            sum += nums[j];
            if sum >= k {
                let length = (j - i + 1) as i32;
                if length < shortest {
                    shortest = length;
                }
            }
        }
    }
    if shortest == i32::MAX {
        -1
    } else {
        shortest
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    #[test]
    fn example() {
        let result = shortest_subarray(vec![1], 1);
        assert_eq!(result, 1);
        let result = shortest_subarray(vec![1, 2], 4);
        assert_eq!(result, -1);
        let result = shortest_subarray(vec![2, -1, 2], 3);
        assert_eq!(result, 3);
    }
    #[test]
    fn example_naive() {
        let result = shortest_subarray_naive(vec![1], 1);
        assert_eq!(result, 1);
        let result = shortest_subarray_naive(vec![1, 2], 4);
        assert_eq!(result, -1);
        let result = shortest_subarray_naive(vec![2, -1, 2], 3);
        assert_eq!(result, 3);
    }

    fn random_vec(rng: &mut impl Rng, length: usize) -> Vec<i32> {
        let mut result : Vec<i32> = Vec::with_capacity(length);
        for _ in 0..length {
            let number : i32 = rng.gen::<i32>() % 10;
            result.push(number)
        }
        result
    }

    #[test]
    fn hard1() {
        let result = shortest_subarray(vec![-9, 8], 1);
        assert_eq!(result, 1);
    }

    #[test]
    fn hard2() {
        let result = shortest_subarray(vec![5, -3, 0, 8, 6, 0, -4, 7, 2], 12);
        assert_eq!(result, 2);
    }

    #[test]
    fn test_random() {
        let mut rng = rand::thread_rng();
        let n2: u16 = rng.gen();
        for _ in 0..1000 {
            let length: usize = rng.gen::<usize>() % 9 + 1;
            let vec = random_vec(&mut rng, length);
            let k = (rng.gen::<i32>() % 100).abs() + 1;
            let res = shortest_subarray(vec.clone(), k);
            let naive = shortest_subarray_naive(vec.clone(), k);
            assert_eq!(res, naive, "Res: {}, naive: {}, k: {}, vec: {:?}", res, naive, k, vec);
        }
    }
}

fn main() {
    shortest_subarray(vec![1, 2, 3, 4, 5], 0);
    println!("Hello, world! {}", shortest_subarray(vec![1], 1));
}
