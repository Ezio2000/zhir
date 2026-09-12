use std::{sync::Arc, time::Instant};
use zhir_core::{
    message::Message,
    run::{Checkpoint, Fact, History, Metrics, RunContext, State},
};

fn trace(count: usize) -> Vec<Arc<Checkpoint>> {
    let mut current = Checkpoint {
        options: zhir_kernel::defaults::run_options(),
        id: "initial".into(),
        parent_id: None,
        revision: 0,
        context: RunContext::new("trace-benchmark", 0),
        history: History::new(vec![Message::user("initial")]).unwrap(),
        state: State::Planning {
            provider_turn_pending: false,
        },
        metrics: Metrics::default(),
        fact: Fact::Started,
    };
    let mut trace = vec![Arc::new(current.clone())];
    for i in 0..count {
        current.parent_id = Some(current.id.clone());
        current.id = format!("c{i}");
        current.revision += 1;
        current.history = current
            .history
            .append(vec![Message::external("fixed size message")])
            .unwrap();
        current.fact = Fact::ConversationInsert {
            source: "host".into(),
        };
        trace.push(Arc::new(current.clone()));
    }
    trace
}
fn main() {
    for count in [1000, 2000, 4000] {
        let trace = trace(count);
        let mut samples: Vec<_> = (0..3)
            .map(|_| {
                let start = Instant::now();
                zhir_kernel::diagnostics::verify_trace(std::hint::black_box(&trace)).unwrap();
                start.elapsed().as_secs_f64() * 1000.0
            })
            .collect();
        samples.sort_by(f64::total_cmp);
        println!(
            "{}",
            serde_json::json!({"case":"verify_trace","transitions":count,"median_ms":samples[1]})
        );
    }
}
