use std::{
    sync::Arc,
    task::{Context, Poll, Waker},
};
use zhir_core::{
    message::{Message, Output},
    run::{Checkpoint, HistoryReducer},
};
use zhir_policies::history::HistoryWindow;

fn checkpoint(messages: Vec<Message>) -> Arc<Checkpoint> {
    let mut checkpoint = zhir_testing::checkpoint(messages);
    checkpoint.active.session.response_status = Some(zhir_core::model::ResponseStatus::Completed);
    Arc::new(checkpoint)
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
        rewrite
            .entries
            .into_iter()
            .map(|entry| entry.message)
            .collect::<Vec<_>>(),
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
