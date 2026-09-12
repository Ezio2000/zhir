use std::sync::Arc;
use zhir::{
    Runtime,
    message::{Content, Message},
    models::{ModelConfig, openai},
    run::State,
    stores::sqlite::SqliteRunStore,
};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let model = openai::chat::model(ModelConfig::new(
        std::env::var("OPENAI_BASE_URL").unwrap_or_else(|_| "https://api.openai.com/v1".into()),
        std::env::var("OPENAI_API_KEY")?,
        std::env::var("OPENAI_MODEL")?,
    ))?;
    let store = Arc::new(SqliteRunStore::connect("sqlite://runs.db?mode=rwc").await?);
    let mut run = Runtime::builder(Arc::new(model))
        .store(store.clone())
        .build()?
        .start(zhir::RunRequest::new(vec![Message::user(
            std::env::args().nth(1).unwrap_or_else(|| "Hello".into()),
        )]))?;
    let checkpoint = run.result().await?.into_checkpoint();
    if let State::Completed { content } = &checkpoint.state {
        println!(
            "{}",
            content
                .iter()
                .filter_map(Content::as_text)
                .collect::<Vec<_>>()
                .join("")
        );
    }
    store.close().await;
    Ok(())
}
