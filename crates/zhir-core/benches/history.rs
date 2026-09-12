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
    for count in [1000, 2000, 4000] {
        let mut samples: Vec<_> = (0..3)
            .map(|_| pending_serial(count).as_secs_f64() * 1000.0)
            .collect();
        samples.sort_by(f64::total_cmp);
        println!(
            "{}",
            serde_json::json!({"case":"pending_serial_append_validate_encode","calls":count,"median_ms":samples[1]})
        );
    }
}

fn pending_serial(count: usize) -> std::time::Duration {
    use zhir_core::{
        message::Output,
        run::{ActiveState, State, validate_history},
        tool::{RuntimeToolCall, RuntimeToolInput, RuntimeToolOutcome},
    };
    let calls: Vec<_> = (0..count)
        .map(|i| RuntimeToolCall {
            id: format!("c{i}"),
            name: "echo".into(),
            input: RuntimeToolInput::Structured(serde_json::json!({"i":i})),
        })
        .collect();
    let mut history = History::new(vec![
        Message::user("initial"),
        Message::Assistant {
            output: calls
                .iter()
                .cloned()
                .map(|call| Output::RuntimeToolCall { call })
                .collect(),
            provider_data: serde_json::Value::Null,
        },
    ])
    .unwrap();
    let mut snapshots = Vec::new();
    let start = Instant::now();
    for call in calls {
        let cursor = history.pending().unwrap().unwrap();
        validate_history(
            &history,
            Some(&ActiveState::RuntimeToolsPending {
                calls: cursor,
                provider_turn_pending: false,
            }),
        )
        .unwrap();
        std::hint::black_box(
            serde_json::to_vec(&State::RuntimeToolsPending {
                calls: cursor,
                provider_turn_pending: false,
            })
            .unwrap(),
        );
        history = history
            .append(vec![Message::RuntimeTool {
                call_id: call.id,
                name: call.name,
                outcome: RuntimeToolOutcome::Success {
                    content: vec![],
                    structured: serde_json::Value::Null,
                },
            }])
            .unwrap();
        snapshots.push(history.clone());
    }
    let elapsed = start.elapsed();
    assert!(history.pending().unwrap().is_none());
    assert_eq!(snapshots.len(), count);
    elapsed
}
