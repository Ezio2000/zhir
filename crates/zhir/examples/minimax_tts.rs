//! Live example: MINIMAX_API_KEY=... cargo run -p zhir --example minimax_tts --features minimax,memory
use std::sync::Arc;
use zhir::resource::MediaReceiver;
use zhir::{
    RunRequest, Runtime,
    message::Message,
    models::{
        credentials::StaticCredential,
        minimax::tts::{self, TtsConfig},
    },
    run::RunMode,
    stores::MemoryResourceStore,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let credentials = Arc::new(StaticCredential::new(
        "Bearer",
        std::env::var("MINIMAX_API_KEY")?,
    ));
    let model = tts::model(TtsConfig::new(
        "speech-2.8-hd",
        "male-qn-qingse",
        credentials,
    ))?;
    let runtime = Runtime::builder(Arc::new(model))
        .resources(Arc::new(MemoryResourceStore::new()))
        .build()?;
    let mut invocation =
        runtime.start(RunRequest::new([Message::user("你好。")]).mode(RunMode::Interactive))?;
    let control = invocation.control();
    let mut audio = invocation.media_output()?;
    let consume = async {
        let mut continued = false;
        while let Some(chunk) = audio.receive().await? {
            // Send these bytes to a player/writer grouped by (stream_id, epoch).
            println!(
                "{} epoch={} sequence={} bytes={} end={}",
                chunk.stream_id,
                chunk.epoch,
                chunk.sequence,
                chunk.bytes.len(),
                chunk.end
            );
            if !continued && !chunk.bytes.is_empty() {
                control
                    .input(Message::user("这是追加的文字"), "example")
                    .await?;
                control.end_input().await?;
                continued = true;
            }
        }
        zhir::Result::Ok(())
    };
    let (completion, consumed) = tokio::join!(invocation.result(), consume);
    consumed?;
    println!("{}", completion?.checkpoint().state.kind());
    Ok(())
}
