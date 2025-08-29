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
        fn remove_recursive(node: &mut Option<Box<Node>>, val: i32) -> bool {
            match node {
                None => return false,
                Some(n) => match val.cmp(&n.value) {
                    Ordering::Less => remove_recursive(&mut n.left, val),
                    Ordering::Greater => remove_recursive(&mut n.right, val),
                    Ordering::Equal => {
                        if let Some(child) = n.left.take() {
                            *node = Some(child);
                            true
                        } else if let Some(child) = n.right.take() {
                            *node = Some(child);
                            true
                        } else {
                            *node = None;
                            true
                        }
                    }
                },
            }
        }
        remove_recursive(&mut self.root, val)
    }
}
