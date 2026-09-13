use serde_json::json;
use zhir_core::{error::Failure, message::Content, tool::RuntimeToolResult};
use zhir_tools::{ToolReply, reply};

#[test]
fn typed_and_json_replies_share_text_and_waiting_semantics() {
    for value in [json!("hello"), json!({"answer":42}), json!([1, 2])] {
        let plain = reply::json(value.clone());
        let typed = ToolReply::success(value.clone()).into_result().unwrap();
        assert_eq!(plain, typed);
        let waiting = reply::waiting("wait", value.clone(), "tool");
        let typed = ToolReply::waiting("wait", value, "tool")
            .into_result()
            .unwrap();
        assert_eq!(waiting, typed);
        assert_eq!(waiting.outcome.content(), plain.outcome.content());
        waiting.validate().unwrap();
    }
    assert_eq!(
        reply::json(json!("hello")).outcome.content(),
        &[Content::text("hello")]
    );
    assert!(reply::waiting("", json!({}), "tool").validate().is_err());
    assert!(
        RuntimeToolResult::failure(Failure::new("failed", "details"))
            .outcome
            .content()
            .is_empty()
    );
}
