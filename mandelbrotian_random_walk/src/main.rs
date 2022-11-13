use rand::Rng;

fn calc(data: &Vec<f64>) -> (f64, f64, f64) {
    let cnt = data.len();
    let sum: f64 = data.iter().sum();
    let abs_sum: f64 = data.iter().map(|x| x.abs()).sum();
    let mean = sum / cnt as f64;
    let variance = data
        .iter()
        .map(|value| {
            let diff = mean - (*value as f64);

            diff * diff
        })
        .sum::<f64>()
        / cnt as f64;
    let stddev = variance.sqrt();
    return (abs_sum, mean, stddev);
}

fn print(data: Vec<f64>, name: String) {
    let (abs_sum, mean, stddev) = calc(&data);
    let cnt = data.len();
    println!("{} mean: {}, stddev: {}", name, mean, stddev);
    for sigma in 1..10 {
        let mut dev_count = 0;
        let mut dev_sum: f64 = 0.0;
        for &val in &data {
            if val > mean + sigma as f64 * stddev {
                dev_count += 1;
                dev_sum += val;
            }
        }
        println!(
            "{} deviations count for {} sigmas: {}, ratio: {}, percent of total: {}",
            name,
            sigma,
            dev_count,
            dev_count as f64 / cnt as f64,
            dev_sum / abs_sum,
        );
    }
}

fn main() {
    let steps = 100;
    let mut reps = 10000;
    loop {
        println!("Reps: {}/n/n/n/n", reps);
        let mut gauss_vals: Vec<f64> = Vec::new();
        let mut mb_vals: Vec<f64> = Vec::new();
        let mut rng = rand::thread_rng();
        for _ in 0..reps {
            let mut gauss_val = 0.0;
            let mut mb_val = 2.0f64;
            for _ in 0..steps {
                // gaussian random walk
                if rng.gen::<bool>() {
                    gauss_val += 1.0;
                    mb_val = mb_val.powf(1.001);
                } else {
                    gauss_val -= 1.0;
                    mb_val = mb_val.powf(0.999);
                }
            }
            gauss_vals.push(gauss_val);
            mb_vals.push(mb_val);
        }
        print(gauss_vals, "Gauss".to_string());
        print(mb_vals, "Mandelbrot".to_string());
        reps *= 10;
    }
}
