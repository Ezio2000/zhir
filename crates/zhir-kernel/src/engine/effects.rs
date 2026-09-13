use super::*;

impl Engine {
    pub(super) fn cancel_active(&self, id: &str) {
        if let Some(token) = self.active.lock().expect("active tool lock").get(id) {
            token.cancel();
            self.emitter
                .emit(EventData::RuntimeToolCancelRequested { call_id: id.into() });
        }
    }
    pub(super) fn cancel_all(&self) {
        for token in self.active.lock().expect("active tool lock").values() {
            token.cancel();
        }
    }
    pub(super) fn queue(&mut self, c: Control) -> Result<()> {
        match c {
            Control::Pause(s) => {
                if self.pause.is_none() {
                    self.pause = Some(s);
                }
            }
            Control::Insert(m, s) => self.inserts.push_back((m, s)),
            Control::CancelTool(id) => self.cancel_active(&id),
            Control::Abort => return Err(Error::Cancelled),
        };
        Ok(())
    }
    pub(super) async fn effect<T>(
        &mut self,
        mut future: BoxFuture<'_, Result<T>>,
        cancellation: Cancellation,
        interrupt: bool,
        defer_insert: bool,
    ) -> Effect<T> {
        let deadline = self.deadline;
        let timer = async move {
            if let Some(d) = deadline {
                tokio::time::sleep_until(d).await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::pin!(timer);
        loop {
            tokio::select! {biased;
                _=&mut timer=>{cancellation.cancel();self.cancel_all();return Effect::Deadline;}
                result=&mut future=>return Effect::Done(result),
                command=self.controls.recv()=>{
                    match command {
                        Some(Control::Abort)|None=>{cancellation.cancel();self.cancel_all();return Effect::Aborted;}
                        Some(Control::CancelTool(id))=>self.cancel_active(&id),
                        Some(c@Control::Pause(_)) if interrupt=>{cancellation.cancel();return Effect::Interrupted(c);}
                        Some(c@Control::Insert(_,_)) if interrupt && !defer_insert=>{cancellation.cancel();return Effect::Interrupted(c);}
                        Some(c)=>{if self.queue(c).is_err() {cancellation.cancel();return Effect::Aborted;}},
                    }
                }
            }
        }
    }
    pub(super) async fn interruption<T>(&mut self, effect: Effect<T>) -> Result<Option<T>> {
        match effect {
            Effect::Done(value) => value.map(Some),
            Effect::Deadline => {
                self.limit(LimitReason::Deadline).await?;
                Ok(None)
            }
            Effect::Aborted => Err(Error::Cancelled),
            Effect::Interrupted(Control::Pause(s)) => {
                if self.expired() {
                    self.limit(LimitReason::Deadline).await?;
                } else {
                    self.suspend(s).await?;
                }
                Ok(None)
            }
            Effect::Interrupted(Control::Insert(m, source)) => {
                if self.expired() {
                    self.limit(LimitReason::Deadline).await?;
                } else {
                    self.advance(
                        State::Planning {
                            provider_turn_pending: false,
                        },
                        Fact::ConversationInsert { source },
                        HistoryDelta::Append(vec![m]),
                        None,
                    )
                    .await?;
                }
                Ok(None)
            }
            Effect::Interrupted(_) => Err(Error::Protocol("invalid effect interruption".into())),
        }
    }
}
