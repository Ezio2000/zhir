use super::*;

struct ActiveGuard {
    id: String,
    active: ActiveTools,
    token: Cancellation,
}
impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.token.cancel();
        self.active
            .lock()
            .expect("active tool lock")
            .remove(&self.id);
    }
}
pub(super) async fn run_tool(
    index: usize,
    call: RuntimeToolCall,
    binding: Arc<dyn RuntimeToolBinding>,
    run: zhir_core::run::RunContext,
    emitter: Emitter,
    active: ActiveTools,
    progress_limit: usize,
) -> (usize, RuntimeToolResult) {
    let token = Cancellation::default();
    active
        .lock()
        .expect("active tool lock")
        .insert(call.id.clone(), token.clone());
    let _guard = ActiveGuard {
        id: call.id.clone(),
        active,
        token: token.clone(),
    };
    emitter.emit(EventData::RuntimeToolStarted {
        call_id: call.id.clone(),
    });
    let (sender, mut receiver) = mpsc::channel(progress_limit.max(1));
    let context = RuntimeToolContext {
        run,
        cancellation: token,
        progress: Some(Arc::new(Progress { sender })),
    };
    let mut future = binding.invoke(context);
    let invoked = loop {
        tokio::select! {result=&mut future=>break result,value=receiver.recv()=>{if let Some(value)=value {emitter.emit(EventData::RuntimeToolProgress {call_id:call.id.clone(),value});}}}
    };
    while let Ok(value) = receiver.try_recv() {
        emitter.emit(EventData::RuntimeToolProgress {
            call_id: call.id.clone(),
            value,
        });
    }
    let result = match invoked {
        Ok(r) => match r.validate() {
            Ok(()) => r,
            Err(e) => RuntimeToolResult::failure(crate::failure::failure(&e)),
        },
        Err(e) => RuntimeToolResult::failure(crate::failure::failure(&e)),
    };
    emitter.emit(EventData::RuntimeToolFinished {
        call_id: call.id,
        outcome: result.outcome.kind(),
    });
    (index, result)
}
