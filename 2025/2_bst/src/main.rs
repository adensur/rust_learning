use bst::Bst;

fn main() {
    let mut tree = Bst::new();
    println!("Contains: {}", tree.contains(1));
    tree.insert(1);
    println!("Contains: {}", tree.contains(1));
    tree.remove(1);
    println!("Contains: {}", tree.contains(1));
}
