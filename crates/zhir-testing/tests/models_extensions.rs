use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use zhir_core::{
    Result,
    error::Error,
    model::{ModelDelta, ModelRequest, TurnOutput},
};
use zhir_models::{ExtensionChain, Protocol, ProtocolExtension, transport::SseEvent};
struct Stage {
    id: u64,
    fail: bool,
    calls: Arc<Mutex<Vec<u64>>>,
}
impl ProtocolExtension for Stage {
    fn encode_request(&mut self, _: Protocol, _: &ModelRequest, body: &mut Value) -> Result<()> {
        self.calls.lock().unwrap().push(self.id);
        body["trace"].as_array_mut().unwrap().push(self.id.into());
        if self.fail {
            Err(Error::Invalid("request hook".into()))
        } else {
            Ok(())
        }
    }
    fn decode_event(&mut self, _: Protocol, event: &mut SseEvent) -> Result<Vec<ModelDelta>> {
        self.calls.lock().unwrap().push(self.id);
        event.data.push_str(&self.id.to_string());
        if self.fail {
            return Err(Error::Protocol("event hook".into()));
        }
        Ok(vec![ModelDelta::Text {
            output_index: 0,
            text: event.data.clone(),
        }])
    }
    fn decode_response(
        &mut self,
        _: Protocol,
        _: &Value,
        decoded: Result<TurnOutput>,
    ) -> Result<TurnOutput> {
        self.calls.lock().unwrap().push(self.id);
        let mut response = decoded?;
        response.provider_data["trace"]
            .as_array_mut()
            .unwrap()
            .push(self.id.into());
        if self.fail {
            Err(Error::Protocol("response hook".into()))
        } else {
            Ok(response)
        }
    }
}
fn request() -> ModelRequest {
    ModelRequest {
        messages: vec![],
        runtime_tools: vec![],
        provider_tools: vec![],
        profile: Default::default(),
        tool_choice: Default::default(),
        response_format: None,
        stream: false,
    }
}
#[test]
fn chain_preserves_order_for_each_hook_and_stops_request_event_failures() {
    for fail in [false, true] {
        let calls = Arc::new(Mutex::new(vec![]));
        let mut chain = ExtensionChain::new();
        for id in 1..=3 {
            chain = chain.push(Stage {
                id,
                fail: fail && id == 2,
                calls: calls.clone(),
            });
        }
        let expected = if fail { vec![1, 2] } else { vec![1, 2, 3] };
        let mut body = json!({"trace":[]});
        assert_eq!(
            chain
                .encode_request(Protocol::Chat, &request(), &mut body)
                .is_err(),
            fail
        );
        assert_eq!(*calls.lock().unwrap(), expected);
        assert_eq!(body["trace"], json!(expected));
        calls.lock().unwrap().clear();
        let mut event = SseEvent {
            data: "".into(),
            ..Default::default()
        };
        let deltas = chain.decode_event(Protocol::Chat, &mut event);
        assert_eq!(deltas.is_err(), fail);
        assert_eq!(*calls.lock().unwrap(), expected);
        if !fail {
            let deltas = deltas.unwrap();
            assert!(matches!(&deltas[2],ModelDelta::Text {text,..} if text=="123"));
        }
        calls.lock().unwrap().clear();
        let mut response = TurnOutput::text("ok");
        response.provider_data = json!({"trace":[]});
        let result = chain.decode_response(Protocol::Chat, &Value::Null, Ok(response));
        assert_eq!(result.is_err(), fail);
        assert_eq!(*calls.lock().unwrap(), vec![1, 2, 3]);
        if !fail {
            assert_eq!(result.unwrap().provider_data["trace"], json!([1, 2, 3]));
        }
    }
}
struct Pass;
impl ProtocolExtension for Pass {}
struct Recover;
impl ProtocolExtension for Recover {
    fn decode_response(
        &mut self,
        _: Protocol,
        raw: &Value,
        decoded: Result<TurnOutput>,
    ) -> Result<TurnOutput> {
        match decoded {
            Err(Error::Protocol(_)) if raw["future_answer"].is_string() => {
                let mut result = TurnOutput::text(raw["future_answer"].as_str().unwrap());
                result.provider_data = json!({"trace":[]});
                Ok(result)
            }
            result => result,
        }
    }
}
#[test]
fn response_recovery_survives_unrelated_stages_and_empty_chain_is_identity() {
    let calls = Arc::new(Mutex::new(vec![]));
    let mut chain = ExtensionChain::new().push(Pass).push(Recover).push(Stage {
        id: 9,
        fail: false,
        calls,
    });
    let result = chain
        .decode_response(
            Protocol::Responses,
            &json!({"future_answer":"ok"}),
            Err(Error::Protocol("new response shape".into())),
        )
        .unwrap();
    assert_eq!(result.provider_data["trace"], json!([9]));
    let mut empty = ExtensionChain::new();
    assert!(
        matches!(empty.decode_response(Protocol::Chat,&Value::Null,Err(Error::Invalid("original".into()))),Err(Error::Invalid(e)) if e=="original")
    );
    assert!(
        empty
            .decode_event(Protocol::Chat, &mut SseEvent::default())
            .unwrap()
            .is_empty()
    );
}
