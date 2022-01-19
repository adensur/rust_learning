#![forbid(unsafe_code)]

use std::cmp::Ordering;
use std::collections::VecDeque;
struct Node {
    key: i64,
    left_ptr: Option<Box<Node>>,
    right_ptr: Option<Box<Node>>,
}

#[derive(Default)]
pub struct BstSet {
    root: Option<Box<Node>>,
    len: usize,
}

impl BstSet {
    pub fn new() -> Self {
        BstSet { root: None, len: 0 }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len > 0
    }

    pub fn contains(&self, key: i64) -> bool {
        let mut option = &self.root;
        while let Some(cur) = option {
            match cur.key.cmp(&key) {
                Ordering::Equal => return true,
                Ordering::Less => {
                    option = &cur.right_ptr;
                }
                Ordering::Greater => {
                    option = &cur.left_ptr;
                }
            }
        }
        false
    }

    fn find_node_mut(&mut self, key: i64) -> &mut Option<Box<Node>> {
        let mut node_ptr = &mut self.root;
        while let Some(node) = node_ptr {
            match node.key.cmp(&key) {
                Ordering::Equal => break,
                Ordering::Less => {
                    //node_ptr = &mut node_ptr.as_mut().unwrap().right_ptr;
                    node_ptr = &mut node.right_ptr;
                }
                Ordering::Greater => {
                    // node_ptr = &mut node_ptr.as_mut().unwrap().left_ptr;
                    node_ptr = &mut node.left_ptr;
                }
            }
        }
        node_ptr
    }

    pub fn insert(&mut self, key: i64) -> bool {
        let mut node_ptr = &mut self.root;
        while let Some(node) = node_ptr {
            match node.key.cmp(&key) {
                Ordering::Equal => return false,
                Ordering::Less => {
                    node_ptr = &mut node.right_ptr;
                }
                Ordering::Greater => {
                    node_ptr = &mut node.left_ptr;
                }
            }
        }
        *node_ptr = Some(Box::new(Node {
            key,
            left_ptr: None,
            right_ptr: None,
        }));
        self.len += 1;
        true
    }

    pub fn insert2(&mut self, key: i64) -> bool {
        let node_ptr = self.find_node_mut(key);
        if node_ptr.is_some() {
            // already have this key!
            return false;
        }
        *node_ptr = Some(Box::new(Node {
            key,
            left_ptr: None,
            right_ptr: None,
        }));
        self.len += 1;
        true
    }

    pub fn remove(&mut self, key: i64) -> bool {
        fn extract_min_node(mut node_ptr: &mut Option<Box<Node>>) -> Option<Box<Node>> {
            while let Some(node) = node_ptr {
                if node.left_ptr.is_some() {
                    node_ptr = &mut node_ptr.as_mut().unwrap().left_ptr;
                    // won't compile:
                    //node_ptr = &mut node.left_ptr;
                } else {
                    break;
                }
            }
            /*loop {
                if node_ptr.is_some() {
                    let node = node_ptr.as_mut().unwrap();
                    if node.left_ptr.is_some() {
                        node_ptr = &mut node.left_ptr;
                        continue;
                    }
                }
                break;
            }*/
            let mut extracted = node_ptr.take();
            if extracted.is_some() {
                *node_ptr = extracted.as_mut().unwrap().right_ptr.take();
            }
            extracted
        }
        // If the key is contained in set, remove it and return true.
        // Otherwise return false.
        let node_ptr = self.find_node_mut(key);
        if let Some(node) = node_ptr {
            let mut right_subtree_min_owned = extract_min_node(&mut node.right_ptr);
            if right_subtree_min_owned.is_some() {
                right_subtree_min_owned.as_mut().unwrap().left_ptr =
                    node_ptr.as_mut().unwrap().left_ptr.take();
                if node_ptr.as_ref().unwrap().right_ptr.is_some() {
                    // node_ptr.right is always null! It was moved from
                    right_subtree_min_owned.as_mut().unwrap().right_ptr =
                        node_ptr.as_mut().unwrap().right_ptr.take();
                }
                *node_ptr = Some(right_subtree_min_owned.unwrap());
            } else {
                // no right subtree
                // move left subtree 1 level up
                *node_ptr = node.left_ptr.take();
            }
            self.len -= 1;
            true
        } else {
            false // couldn't find the key
        }
    }

    pub fn print(&mut self) {
        fn make_offset(offset: i64) -> String {
            assert!(offset > 0);
            let mut result = String::new();
            for _ in 0..offset {
                result.push(' ');
            }
            result
        }
        if let Some(root) = &self.root {
            let mut queue: VecDeque<(&Box<Node>, i64)> = VecDeque::new();
            queue.push_back((root, 2 * self.len as i64));
            while let Some((node, offset)) = queue.pop_front() {
                println!("{}{}", make_offset(offset), node.key);
                if let Some(left) = &node.left_ptr {
                    queue.push_back((left, offset / 2));
                }
                if let Some(right) = &node.right_ptr {
                    queue.push_back((right, 3 * (offset / 2)));
                }
            }
        }
    }
}
