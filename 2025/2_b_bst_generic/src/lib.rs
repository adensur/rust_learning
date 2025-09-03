use std::cmp::Ordering;

pub struct Node<K, V> {
    key: K,
    val: V,
    left: Option<Box<Node<K, V>>>,
    right: Option<Box<Node<K, V>>>,
}

pub struct Bst<K, V> {
    root: Option<Box<Node<K, V>>>,
}

impl<K: Ord, V> Bst<K, V> {
    pub fn new() -> Self {
        Self { root: None }
    }

    fn insert_recursive(node: &mut Option<Box<Node<K, V>>>, k: K, v: V) -> Option<V> {
        match node {
            None => {
                *node = Some(Box::new(Node {
                    key: k,
                    val: v,
                    left: None,
                    right: None,
                }));
                None
            }
            Some(node) => match k.cmp(&node.key) {
                Ordering::Equal => {
                    // let old_v = node.val;
                    let old_v = std::mem::replace(&mut node.val, v);
                    Some(old_v)
                }
                Ordering::Less => Self::insert_recursive(&mut node.left, k, v),
                Ordering::Greater => Self::insert_recursive(&mut node.right, k, v),
            },
        }
    }

    pub fn insert(&mut self, k: K, v: V) -> Option<V> {
        Self::insert_recursive(&mut self.root, k, v)
    }

    fn contains_key_recursive(node: &Option<Box<Node<K, V>>>, k: &K) -> bool {
        match node {
            None => false,
            Some(node) => match k.cmp(&node.key) {
                Ordering::Equal => true,
                Ordering::Less => Self::contains_key_recursive(&node.left, k),
                Ordering::Greater => Self::contains_key_recursive(&node.right, k),
            },
        }
    }

    pub fn contains_key(&self, k: &K) -> bool {
        Self::contains_key_recursive(&self.root, k)
    }

    fn pop_min(mut node: Box<Node<K, V>>) -> (Box<Node<K, V>>, Option<Box<Node<K, V>>>) {
        if let Some(child) = node.left {
            let (min, subtree) = Self::pop_min(child);
            node.left = subtree;
            (min, Some(node))
        } else {
            let right = node.right.take();
            (node, right)
        }
    }

    fn remove_recursive(node: &mut Option<Box<Node<K, V>>>, k: &K) -> Option<V> {
        match node {
            None => None,
            Some(cur) => match k.cmp(&cur.key) {
                Ordering::Less => Self::remove_recursive(&mut cur.left, k),
                Ordering::Greater => Self::remove_recursive(&mut cur.right, k),
                Ordering::Equal => match (cur.left.take(), cur.right.take()) {
                    (None, None) => {
                        let old_node = std::mem::replace(node, None).unwrap();
                        Some(old_node.val)
                    }
                    (Some(child), None) => {
                        let old_node = std::mem::replace(node, Some(child)).unwrap();
                        Some(old_node.val)
                    }
                    (None, Some(child)) => {
                        let old_node = std::mem::replace(node, Some(child)).unwrap();
                        Some(old_node.val)
                    }
                    (Some(child1), Some(child2)) => {
                        let (mut min, subtree) = Self::pop_min(child2);
                        min.left = Some(child1);
                        min.right = subtree;
                        let old_node = std::mem::replace(node, Some(min)).unwrap();
                        Some(old_node.val)
                    }
                },
            },
        }
    }

    pub fn remove(&mut self, k: &K) -> Option<V> {
        Self::remove_recursive(&mut self.root, k)
    }
}

impl<K: Ord + Clone, V: Clone> Bst<K, V> {
    fn append_to_sorted_vec_recursive(
        node: &mut Option<Box<Node<K, V>>>,
        result: &mut Vec<(K, V)>,
    ) {
        match node {
            None => return,
            Some(cur) => {
                Self::append_to_sorted_vec_recursive(&mut cur.left, result);
                result.push((cur.key.clone(), cur.val.clone()));
                Self::append_to_sorted_vec_recursive(&mut cur.right, result);
            }
        }
    }

    fn to_sorted_vec(&mut self) -> Vec<(K, V)> {
        let mut result = Vec::new();
        Self::append_to_sorted_vec_recursive(&mut self.root, &mut result);
        result
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rand::Rng;

    use super::*;

    fn map_to_sorted_vec<K, V>(map: BTreeMap<K, V>) -> Vec<(K, V)> {
        let mut result = Vec::new();
        for (k, v) in map {
            result.push((k, v));
        }
        result
    }

    #[test]
    fn test1() {
        let mut rng = rand::rng();
        let mut bst = Bst::new();
        let mut std_map = BTreeMap::new();
        for i in 0..=10000 {
            let number = rng.random_range(0..=10);
            let value = format!("{number}");
            let action_is_insert = rng.random_bool(0.5);
            if action_is_insert {
                bst.insert(number, value.clone());
                std_map.insert(number, value);
            } else {
                bst.remove(&number);
                std_map.remove(&number);
            }
            assert_eq!(bst.to_sorted_vec(), map_to_sorted_vec(std_map.clone()));
        }
    }
}
