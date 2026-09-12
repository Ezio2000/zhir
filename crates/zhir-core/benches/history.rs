use std::time::Instant;
use zhir_core::{message::Message, run::History};
fn run(count: usize) -> std::time::Duration {
    let started = Instant::now();
    let mut history = History::new(vec![Message::user("initial")]).unwrap();
    let first = history.clone();
    for _ in 0..count {
        history = history
            .append(vec![Message::external("fixed size message")])
            .unwrap();
        std::hint::black_box(&history);
    }
    let elapsed = started.elapsed();
    assert_eq!(history.len(), count + 1);
    assert_eq!(first.len(), 1);
    assert_eq!(history.messages().len(), count + 1);
    elapsed
}
fn main() {
    let a = run(10_000);
    let b = run(20_000);
    println!(
        "history append: 10000={a:?}, 20000={b:?}, ratio={:.2}",
        b.as_secs_f64() / a.as_secs_f64()
    );
}
