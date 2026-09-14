use serde_json::json;
use zhir_core::operation::OperationOutcome;
use zhir_core::{error::Failure, message::Content};
use zhir_testing::FinalExecution;
use zhir_tools::{ToolReply, reply};
#[test]
fn typed_and_json_replies_share_final_payload_semantics() {
    for value in [json!("hello"), json!({"answer":42}), json!([1, 2])] {
        let plain = reply::json(value.clone());
        let typed = ToolReply::success(value).into_execution().unwrap();
        assert_eq!(plain.final_outcome(), typed.final_outcome());
        plain.final_outcome().validate().unwrap();
    }
    assert_eq!(
        reply::json(json!("hello")).final_outcome().content(),
        &[Content::text("hello")]
    );
    assert!(
        OperationOutcome::Failure {
            error: Failure::new("failed", "details")
        }
        .content()
        .is_empty()
    );
}
