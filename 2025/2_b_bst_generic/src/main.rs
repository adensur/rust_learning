use bst::Bst;
use std::collections::BTreeMap;

fn main() {
    println!("Hello, world!");
    let mut map = BTreeMap::new();
    let mut bst = Bst::new();
    map.insert(1, "whoa!");
    bst.insert(1, "whoa!");
    let verdict = bst.contains_key(&1);
    println!("{verdict}");
    let verdict = bst.contains_key(&2);
    println!("{verdict}");
    let x = bst.remove(&1);
    let verdict = bst.contains_key(&1);
    println!("{verdict}");
}
