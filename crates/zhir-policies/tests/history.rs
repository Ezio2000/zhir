use std::{
    sync::Arc,
    task::{Context, Poll, Waker},
};
use zhir_core::{
    message::{Message, Output},
    run::{
        Checkpoint, Fact, History, HistoryReducer, Limits, Metrics, RunContext, RunOptions, State,
    },
};
use zhir_policies::history::HistoryWindow;

fn checkpoint(messages: Vec<Message>) -> Arc<Checkpoint> {
    Arc::new(Checkpoint {
        options: RunOptions {
            runtime_tools: Default::default(),
            limits: Limits {
                max_planning_steps: 10,
                max_runtime_tool_calls: 10,
                max_runtime_tool_batch_size: 2,
                max_runtime_tool_concurrency: 2,
                max_progress_events: 4,
                max_buffered_progress: 4,
                max_total_tokens: None,
                elapsed_ms: None,
                commit_timeout_ms: 100,
            },
            model: Default::default(),
            provider_tools: vec![],
            tool_choice: Default::default(),
            response_format: None,
            stream: false,
        },
        id: "checkpoint".into(),
        parent_id: None,
        revision: 0,
        context: RunContext::new("run", 0),
        history: History::new(messages).unwrap(),
        state: State::Planning {
            provider_turn_pending: false,
        },
        metrics: Metrics::default(),
        fact: Fact::Started,
    })
}

#[test]
fn window_preserves_system_order_and_honors_expanded_dependencies_without_an_executor() {
    let messages = vec![
        Message::system("first"),
        Message::user("old"),
        Message::Assistant {
            output: vec![Output::text("answer")],
            provider_data: Default::default(),
        },
        Message::system("later"),
        Message::user("current"),
    ];
    let snapshot = checkpoint(messages.clone());
    let window = HistoryWindow::last_turns(1).unwrap();
    let mut future = window.reduce(snapshot.clone());
    let Poll::Ready(Ok(Some(rewrite))) = future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("pure policy must return a rewrite")
    };
    assert_eq!(
        rewrite.messages,
        vec![
            messages[0].clone(),
            messages[3].clone(),
            messages[4].clone()
        ]
    );
    assert_eq!(snapshot.history.messages(), messages);
    let expanded = HistoryWindow::last_turns(1)
        .unwrap()
        .with_dependencies(|_, _| Ok(0));
    let mut future = expanded.reduce(snapshot);
    assert!(matches!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(None))
    ));
}
