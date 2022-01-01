fn swap<'a>(x: &'a mut i32, y: &'a mut i32) {
    let z = *x;
    *x = *y;
    *y = z;
}

fn main() {
    let mut x = 1;
    let mut y = 2;
    println!("x = {}, y = {}", x, y);
    swap(&mut x, &mut y);
    println!("x = {}, y = {}", x, y);
}
