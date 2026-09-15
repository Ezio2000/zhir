//! Native Codex voice smoke client. Host-owned Opus silence clocks the audio track.
//! ZHIR_LIVE_TOKEN / ZHIR_LIVE_ACCOUNT_ID are supplied by the host's login flow.
use std::{sync::Arc, time::Duration};
use zhir::{
    BoxFuture, Result, RunRequest, Runtime,
    credential::{Credential, CredentialContext, CredentialProvider},
    message::Message,
    models::openai::live::{self, AUDIO_TYPE, LiveConfig},
    resource::{MediaChunk, MediaReceiver},
    run::{RunContext, RunMode},
    stores::{MemoryResourceStore, MemoryRunStore},
};
struct Login(Credential);
impl CredentialProvider for Login {
    fn resolve(&self, _: CredentialContext) -> BoxFuture<'_, Result<Credential>> {
        Box::pin(async { Ok(self.0.clone()) })
    }
    fn invalidate(&self, _: &str) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}
#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let login = Login(Credential {
        generation: "example".into(),
        scheme: "Bearer".into(),
        value: std::env::var("ZHIR_LIVE_TOKEN")?,
        expires_at_ms: None,
        metadata: [(
            "header:ChatGPT-Account-Id".into(),
            std::env::var("ZHIR_LIVE_ACCOUNT_ID")?,
        )]
        .into(),
    });
    let mut config = LiveConfig::new(Arc::new(login));
    config.instructions = "Say only: voice connection works.".into();
    let runtime = Runtime::builder(Arc::new(live::model(config)?))
        .store(Arc::new(MemoryRunStore::new()))
        .resources(Arc::new(MemoryResourceStore::new()))
        .build()?;
    let mut invocation = runtime.start(
        RunRequest::new([Message::user("Voice connection test")])
            .context(RunContext::new("voice-example", 0))
            .mode(RunMode::Interactive),
    )?;
    let control = invocation.control();
    let microphone = invocation.media_input();
    let mut output = invocation.media_output()?;
    let input = async {
        let turn = loop {
            if let Some(c) = runtime.load_checkpoint("voice-example").await?
                && let Some(turn) = &c.active.session.turn_id
            {
                break turn.clone();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let mut interval = tokio::time::interval(Duration::from_millis(20));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        for sequence in 0..600 {
            interval.tick().await;
            // Replace this Opus silence packet with encoded microphone audio in a product.
            microphone
                .send(MediaChunk {
                    stream_id: "microphone".into(),
                    turn_id: turn.clone(),
                    epoch: 0,
                    sequence,
                    timestamp_us: sequence * 20000,
                    media_type: AUDIO_TYPE.into(),
                    bytes: vec![0xf8, 0xff, 0xfe],
                    end: sequence == 599,
                })
                .await?;
            if sequence == 150 {
                control
                    .input(
                        Message::user("Please say: voice connection works."),
                        "example",
                    )
                    .await?;
            }
        }
        control.end_input().await?;
        Result::Ok(())
    };
    let receive = async {
        let mut packets = 0;
        while let Some(chunk) = output.receive().await? {
            if !chunk.bytes.is_empty() {
                packets += 1; /* Pass each Opus packet and RTP-derived timestamp to a decoder/player. */
            }
        }
        println!("received {packets} Opus packets");
        Result::Ok(())
    };
    let run = async {
        let (done, input, output) = tokio::join!(invocation.result(), input, receive);
        input?;
        output?;
        let done = done?;
        println!("{}", done.checkpoint().state.kind());
        for message in done.checkpoint().history.messages() {
            if let Message::Assistant { output, .. } = message {
                for content in zhir::message::visible_content(&output) {
                    println!("{content:?}");
                }
            }
        }
        std::result::Result::<(), Box<dyn std::error::Error>>::Ok(())
    };
    tokio::time::timeout(Duration::from_secs(45), run).await??;
    Ok(())
}
