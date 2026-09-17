use std::time::Instant;
use zhir_core::operation::OperationOutcome;
use zhir_core::{
    message::Message,
    run::{History, HistoryEntry},
};
fn run(count: usize) -> std::time::Duration {
    let started = Instant::now();
    let mut history = History::new(vec![Message::user("initial")]).unwrap();
    let first = history.clone();
    for i in 0..count {
        history = history
            .append(vec![HistoryEntry {
                id: format!("external:{i}"),
                origin: None,
                message: Message::external("fixed size message"),
            }])
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
    for count in [1000, 2000, 4000] {
        let mut samples: Vec<_> = (0..3)
            .map(|_| pending_serial(count).as_secs_f64() * 1000.0)
            .collect();
        samples.sort_by(f64::total_cmp);
        println!(
            "{}",
            serde_json::json!({"case":"pending_serial_append_validate","calls":count,"median_ms":samples[1]})
        );
    }
}

fn pending_serial(count: usize) -> std::time::Duration {
    use zhir_core::{
        message::Output,
        operation::CallRef,
        tool::{RuntimeToolCall, RuntimeToolInput},
    };
    let origin = |i| CallRef {
        session_id: "session".into(),
        item_id: format!("c{i}"),
        generation_id: Some("turn".into()),
        caller_id: "model".into(),
        call_id: format!("c{i}"),
    };
    let mut history = History::new(vec![Message::user("initial")]).unwrap();
    for i in 0..count {
        history = history
            .append(vec![HistoryEntry {
                id: format!("call:{i}"),
                origin: Some(origin(i)),
                message: Message::Assistant {
                    output: vec![Output::RuntimeToolCall {
                        call: RuntimeToolCall {
                            id: format!("c{i}"),
                            name: "echo".into(),
                            input: RuntimeToolInput::Structured(serde_json::json!({"i":i})),
                        },
                    }],
                    provider_data: serde_json::Value::Null,
                },
            }])
            .unwrap();
    }
    let mut snapshots = Vec::new();
    let start = Instant::now();
    for i in 0..count {
        history.validate().unwrap();
        std::hint::black_box(history.pending_calls().next().unwrap());
        history = history
            .append(vec![HistoryEntry {
                id: format!("result:{i}"),
                origin: Some(origin(i)),
                message: Message::RuntimeTool {
                    call_id: format!("c{i}"),
                    name: "echo".into(),
                    outcome: OperationOutcome::Success {
                        content: vec![],
                        structured: serde_json::Value::Null,
                    },
                },
            }])
            .unwrap();
        snapshots.push(history.clone());
    }
    let elapsed = start.elapsed();
    assert_eq!(history.pending_calls().count(), 0);
    assert_eq!(snapshots.len(), count);
    elapsed
}
