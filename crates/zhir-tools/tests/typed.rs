#![cfg(feature = "typed")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use zhir_core::{
    error::Error,
    tool::{
        Execution, InputSpec, RuntimeTool, RuntimeToolCall, RuntimeToolCatalogProvider,
        RuntimeToolContext, RuntimeToolInput,
    },
};
use zhir_tools::{RuntimeToolRegistry, TypedTool};

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Address {
    city: String,
}
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Input {
    #[serde(rename = "customer_id")]
    id: String,
    address: Address,
    #[serde(default)]
    quantity: u32,
}
#[derive(Serialize, JsonSchema)]
struct Output {
    #[serde(rename(serialize = "receipt", deserialize = "unused_input_name"))]
    id: String,
    total: u32,
    city: String,
}
fn context() -> RuntimeToolContext {
    RuntimeToolContext {
        run: zhir_core::run::RunContext::new("port-test", 0),
        cancellation: Default::default(),
        progress: None,
    }
}
fn call(value: serde_json::Value) -> RuntimeToolCall {
    RuntimeToolCall {
        id: "c1".into(),
        name: "create".into(),
        input: RuntimeToolInput::Structured(value),
    }
}
#[tokio::test]
async fn generated_schemas_follow_input_and_output_serde_contracts() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let tool = TypedTool::<Input, Output>::new(
        "create",
        "Create receipt",
        Execution::default(),
        move |input, _| {
            counter.fetch_add(1, Ordering::SeqCst);
            async move {
                Ok(zhir_tools::ToolReply::success(Output {
                    id: input.id,
                    total: input.quantity * 2,
                    city: input.address.city,
                }))
            }
        },
    )
    .unwrap();
    let InputSpec::Structured { schema } = &tool.spec().input else {
        panic!()
    };
    assert!(schema["properties"].get("customer_id").is_some());
    assert!(schema["$defs"].get("Address").is_some());
    let output = tool.spec().output_schema.as_ref().unwrap();
    assert!(output["properties"].get("receipt").is_some());
    assert!(output["properties"].get("unused_input_name").is_none());
    let registry =
        RuntimeToolRegistry::from_tools([Arc::new(tool) as Arc<dyn RuntimeTool>]).unwrap();
    let catalog = registry
        .open_catalog(zhir_core::tool::CatalogContext {
            run: zhir_core::run::RunContext::new("port-test", 0),
            cancellation: Default::default(),
        })
        .await
        .unwrap();
    let request = call(json!({"customer_id":"宁筠", "address":{"city":"杭州"}}));
    let result = catalog
        .bind(&request)
        .unwrap()
        .invoke(context())
        .await
        .unwrap();
    assert_eq!(
        result.outcome.structured(),
        Some(&json!({"receipt":"宁筠","total":0,"city":"杭州"}))
    );
    for input in [
        json!({"customer_id":"x","address":{"city":"a","extra":1}}),
        json!({"customer_id":"x","address":{"city":3}}),
        json!({"customer_id":"x"}),
        json!({"customer_id":"x","address":{"city":"a"},"quantity":-1}),
    ] {
        assert!(catalog.bind(&call(input)).is_err());
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // Serde supplies the Rust default; binding never mutates the caller's JSON.
    assert!(
        matches!(&request.input,RuntimeToolInput::Structured(v) if v.get("quantity").is_none())
    );
}
#[tokio::test]
async fn typed_tools_keep_output_validation_and_error_lifecycle() {
    #[derive(Serialize, JsonSchema)]
    struct Wrong {
        #[schemars(with = "String")]
        value: u32,
    }
    let tool =
        TypedTool::<Input, Wrong>::new("create", "fixture", Execution::default(), |_, _| async {
            Ok(zhir_tools::ToolReply::success(Wrong { value: 7 }))
        })
        .unwrap();
    let catalog = RuntimeToolRegistry::from_tools([Arc::new(tool) as Arc<dyn RuntimeTool>])
        .unwrap()
        .open_catalog(zhir_core::tool::CatalogContext {
            run: zhir_core::run::RunContext::new("port-test", 0),
            cancellation: Default::default(),
        })
        .await
        .unwrap();
    let bound = catalog
        .bind(&call(json!({"customer_id":"x","address":{"city":"a"}})))
        .unwrap();
    assert!(matches!(
        bound.invoke(context()).await,
        Err(Error::Validation(
            zhir_core::error::ValidationError::Value { .. }
        ))
    ));
    let invoked = Arc::new(AtomicUsize::new(0));
    let count = invoked.clone();
    let tool = TypedTool::<Input, String>::new(
        "create",
        "fixture",
        Execution::default(),
        move |_, context| {
            count.fetch_add(1, Ordering::SeqCst);
            async move {
                context.cancellation.cancel();
                Ok(zhir_tools::ToolReply::success("late".into()))
            }
        },
    )
    .unwrap();
    let ctx = context();
    ctx.cancellation.cancel();
    assert!(matches!(
        tool.invoke(call(json!({})), ctx).await,
        Err(Error::Cancelled)
    ));
    assert_eq!(invoked.load(Ordering::SeqCst), 0);
    assert!(matches!(
        tool.invoke(
            call(json!({"customer_id":"x","address":{"city":"a"}})),
            context()
        )
        .await,
        Err(Error::Cancelled)
    ));
    assert_eq!(invoked.load(Ordering::SeqCst), 1);
    assert!(
        TypedTool::<Input, String>::new(
            "bad",
            "fixture",
            Execution {
                parallel: true,
                ..Default::default()
            },
            |_, _| async { Ok(zhir_tools::ToolReply::success("".into())) }
        )
        .is_err()
    );
}

#[tokio::test]
async fn typed_replies_preserve_media_and_validate_success_accepted_and_waiting_payloads() {
    use zhir_core::message::{Content, MediaSource};
    use zhir_tools::ToolReply;
    #[derive(Serialize, JsonSchema)]
    struct Receipt {
        #[schemars(with = "String")]
        value: serde_json::Value,
    }
    for kind in 0..3 {
        for valid in [false, true] {
            let tool = TypedTool::<Input, Receipt>::new(
                "create",
                "fixture",
                Execution::default(),
                move |_, _| async move {
                    let payload = Receipt {
                        value: if valid { json!("ok") } else { json!(7) },
                    };
                    let reply = match kind {
                        0 => ToolReply::success(payload),
                        1 => ToolReply::accepted("task-1", payload),
                        _ => ToolReply::waiting("wait-1", payload, "consumer"),
                    };
                    Ok(reply.content([Content::Image {
                        source: MediaSource::Url {
                            url: "https://consumer.example/image".into(),
                        },
                    }]))
                },
            )
            .unwrap();
            let catalog = RuntimeToolRegistry::from_tools([Arc::new(tool) as Arc<dyn RuntimeTool>])
                .unwrap()
                .open_catalog(zhir_core::tool::CatalogContext {
                    run: zhir_core::run::RunContext::new("port-test", 0),
                    cancellation: Default::default(),
                })
                .await
                .unwrap();
            let result = catalog
                .bind(&call(json!({"customer_id":"x","address":{"city":"a"}})))
                .unwrap()
                .invoke(context())
                .await;
            if !valid {
                assert!(matches!(
                    result,
                    Err(Error::Validation(
                        zhir_core::error::ValidationError::Value { .. }
                    ))
                ));
                continue;
            }
            let result = result.unwrap();
            assert_eq!(
                result.outcome.kind(),
                [
                    zhir_core::tool::RuntimeToolOutcomeKind::Success,
                    zhir_core::tool::RuntimeToolOutcomeKind::Accepted,
                    zhir_core::tool::RuntimeToolOutcomeKind::Waiting
                ][kind]
            );
            assert_eq!(result.outcome.structured(), Some(&json!({"value":"ok"})));
            assert!(matches!(
                &result.outcome.content()[0],
                Content::Image { .. }
            ));
            assert_eq!(result.suspension.is_some(), kind == 2);
        }
    }
    assert!(ToolReply::accepted("", json!({})).into_result().is_err());
    assert!(
        ToolReply::waiting("", json!({}), "consumer")
            .into_result()
            .is_err()
    );
    assert!(
        ToolReply::success(json!({}))
            .content([Content::Image {
                source: MediaSource::Url { url: String::new() }
            }])
            .into_result()
            .is_err()
    );
}
