use std::cmp::Ordering;

struct Node {
    left: Option<Box<Node>>,
    right: Option<Box<Node>>,
    value: i32,
}

pub struct Bst {
    root: Option<Box<Node>>,
}

impl Bst {
    pub fn new() -> Self {
        Bst { root: None }
    }

    pub fn insert(&mut self, val: i32) -> bool {
        fn insert_recursive(node: &mut Option<Box<Node>>, val: i32) -> bool {
            match node {
                None => {
                    *node = Some(Box::new(Node {
                        left: None,
                        right: None,
                        value: val,
                    }));
                    true
                }
                Some(node) => match val.cmp(&node.value) {
                    Ordering::Equal => false,
                    Ordering::Less => insert_recursive(&mut node.left, val),
                    Ordering::Greater => insert_recursive(&mut node.right, val),
                },
            }
        }
        insert_recursive(&mut self.root, val)
    }

    pub fn contains(&self, val: i32) -> bool {
        fn contains_recursive(node: &Option<Box<Node>>, val: i32) -> bool {
            match node {
                None => false,
                Some(node) => match val.cmp(&node.value) {
                    Ordering::Equal => true,
                    Ordering::Less => contains_recursive(&node.left, val),
                    Ordering::Greater => contains_recursive(&node.right, val),
                },
            }
        }
        contains_recursive(&self.root, val)
    }

    pub fn remove(&mut self, val: i32) -> bool {
        fn pop_min(mut node: Box<Node>) -> (i32, Option<Box<Node>>) {
            if let Some(left) = node.left.take() {
                let (min, sub) = pop_min(left);
                node.left = sub;
                return (min, Some(node));
            }
            return (node.value, node.right);
        }

        fn remove_recursive(node: &mut Option<Box<Node>>, val: i32) -> bool {
            match node {
                None => return false,
                Some(n) => match val.cmp(&n.value) {
                    Ordering::Less => remove_recursive(&mut n.left, val),
                    Ordering::Greater => remove_recursive(&mut n.right, val),
                    Ordering::Equal => match (n.left.take(), n.right.take()) {
                        (None, None) => {
                            *node = None;
                            true
                        }
                        (Some(child), None) => {
                            *node = Some(child);
                            true
                        }
                        (None, Some(child)) => {
                            *node = Some(child);
                            true
                        }
                        (Some(child1), Some(child2)) => {
                            let (min, sub) = pop_min(child2);
                            n.left = Some(child1);
                            n.right = sub;
                            n.value = min;
                            true
                        }
                    },
                },
            }
        }
        remove_recursive(&mut self.root, val)
    }

    pub fn to_sorted_vec(&self) -> Vec<i32> {
        let mut results = Vec::new();

        fn populate_recursive(cur: &Option<Box<Node>>, results: &mut Vec<i32>) {
            let Some(node) = cur else {
                return;
            };
            populate_recursive(&node.left, results);
            results.push(node.value);
            populate_recursive(&node.right, results);
        }

        populate_recursive(&self.root, &mut results);
        results
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use rand::Rng;

    use super::*;

    #[test]
    fn test1() {
        let mut tree = Bst::new();
        assert!(!tree.contains(1));
        tree.insert(1);
        assert!(tree.contains(1));
        tree.remove(1);
        assert!(!tree.contains(1));
    }

    #[test]
    fn test2() {
        let mut tree = Bst::new();
        let vec1 = tree.to_sorted_vec();
        assert_eq!(vec1, vec![]);
        tree.insert(2);
        tree.insert(1);
        tree.insert(3);
        assert_eq!(tree.to_sorted_vec(), vec![1, 2, 3]);
    }

    #[test]
    fn test3() {
        let mut tree = Bst::new();
        for i in 0..=10 {
            tree.insert(i);
        }
        assert_eq!(tree.to_sorted_vec(), vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        tree.remove(5);
        assert_eq!(tree.to_sorted_vec(), vec![0, 1, 2, 3, 4, 6, 7, 8, 9, 10]);
        tree.remove(3);
        assert_eq!(tree.to_sorted_vec(), vec![0, 1, 2, 4, 6, 7, 8, 9, 10]);
        tree.remove(2);
        assert_eq!(tree.to_sorted_vec(), vec![0, 1, 4, 6, 7, 8, 9, 10]);
        tree.remove(9);
        assert_eq!(tree.to_sorted_vec(), vec![0, 1, 4, 6, 7, 8, 10]);
    }

    #[test]
    fn test4() {
        let mut tree = Bst::new();
        tree.insert(5);
        tree.insert(2);
        tree.insert(7);
        tree.insert(6);
        tree.insert(8);
        assert_eq!(tree.to_sorted_vec(), vec![2, 5, 6, 7, 8]);
        tree.remove(5);
        assert_eq!(tree.to_sorted_vec(), vec![2, 6, 7, 8]);
    }

    fn set_to_vec(set: BTreeSet<i32>) -> Vec<i32> {
        let mut result = Vec::new();
        for elem in set {
            result.push(elem);
        }
        result
    }

    #[test]
    fn test5() {
        // random
        let mut rng = rand::rng();
        let mut std_set = BTreeSet::new();
        let mut bst_set = Bst::new();
        for _i in 1..=1000 {
            let number = rng.random_range(0..=10);
            let action = rng.random_range(1..=2);
            match action {
                1 => {
                    // insert
                    std_set.insert(number);
                    bst_set.insert(number);
                }
                2 => {
                    // remove
                    std_set.remove(&number);
                    bst_set.remove(number);
                }
                _ => unreachable!(),
            }
            assert_eq!(set_to_vec(std_set.clone()), bst_set.to_sorted_vec());
        }
    }
}
