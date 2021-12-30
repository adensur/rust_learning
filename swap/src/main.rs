fn swap(x: &mut i32, y: &mut i32) {
    let z = x;
    x = y;
    y = z;
}

fn main() {
    let mut x = 1;
    let mut y = 2;
    swap(&mut x, &mut y);
    println!("x = {}, y = {}", x, y);
}
