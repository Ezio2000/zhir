use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[derive(Clone, Copy)]
pub enum Fault {
    None,
    Disconnect,
    BadAudio,
    OddAudio,
    OversizedAudio,
    Burst,
}

pub async fn serve(fault: Fault) -> (String, JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}/ws/v1/t2a_v2_bidi", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(socket).await.unwrap();
        socket
            .send(Message::Text(
                json!({"event":"connected_success"}).to_string().into(),
            ))
            .await
            .unwrap();
        let mut commands = vec![];
        let mut inputs = 0;
        let mut interrupted = false;
        while let Some(Ok(Message::Text(text))) = socket.next().await {
            let value: Value = serde_json::from_str(&text).unwrap();
            let event = value["event"].as_str().unwrap();
            commands.push(event.to_owned());
            let replies = match event {
                "task_start" => vec![json!({"event":"task_started"})],
                "task_continue" => {
                    inputs += 1;
                    if inputs == 1 {
                        let mut replies = vec![json!({"event":"sentence_start"})];
                        replies.extend(match fault {
                            Fault::Disconnect => break,
                            Fault::OddAudio => vec![json!({"event":"task_continued","data":{"audio":"0"}})],
                            Fault::OversizedAudio => vec![json!({"event":"task_continued","data":{"audio":"00".repeat(1024*1024+1)}})],
                            Fault::BadAudio => {
                                vec![json!({"event":"task_continued","data":{"audio":"zz"}})]
                            }
                            Fault::None => vec![
                                json!({"event":"task_continued","data":{"audio":"010203"},"is_final":true}),
                            ],
                            Fault::Burst => (0..24).map(|_| json!({"event":"task_continued","data":{"audio":"010203"},"is_final":true})).collect(),
                        });
                        replies.push(
                            json!({"event":"task_continued","data":{"audio":""},"is_final":true}),
                        );
                        replies
                    } else {
                        vec![]
                    }
                }
                // A frame already in flight before the cancellation acknowledgement.
                "task_cancel" => {
                    interrupted = true;
                    vec![
                        json!({"event":"task_continued","data":{"audio":"eeff"}}),
                        json!({"event":"task_canceled"}),
                    ]
                }
                "task_finish" => {
                    assert_eq!(inputs, 3);
                    let mut replies = vec![];
                    if !interrupted {
                        replies.push(json!({"event":"sentence_end"}));
                    }
                    replies.extend([
                        json!({"event":"sentence_start"}),
                        json!({"event":"task_continued","data":{"audio":"04050607"},"is_final":true}),
                        json!({"event":"sentence_end"}),
                        json!({"event":"task_finished"}),
                    ]);
                    replies
                }
                _ => panic!("unexpected client command {event}"),
            };
            for reply in replies {
                socket
                    .send(Message::Text(reply.to_string().into()))
                    .await
                    .unwrap();
            }
            if event == "task_finish"
                || matches!(
                    fault,
                    Fault::BadAudio | Fault::OddAudio | Fault::OversizedAudio
                ) && inputs > 0
            {
                break;
            }
        }
        let _ = socket.close(None).await;
        commands
    });
    (endpoint, task)
}
