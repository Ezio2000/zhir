use std::{
    collections::{BTreeMap, BTreeSet},
    process::Command,
};

#[test]
fn production_dependencies_follow_sdk_boundaries() {
    let output = Command::new(env!("CARGO"))
        .args(["metadata", "--no-deps", "--locked", "--format-version", "1"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let expected: BTreeMap<_, _> = [
        ("zhir-core", vec![]),
        ("zhir-policies", vec!["zhir-core"]),
        ("zhir-kernel", vec!["zhir-core"]),
        ("zhir-storage", vec!["zhir-core"]),
        ("zhir-models", vec!["zhir-core", "zhir-policies"]),
        (
            "zhir-minimax",
            vec!["zhir-core", "zhir-policies", "zhir-models"],
        ),
        (
            "zhir-openai",
            vec!["zhir-core", "zhir-policies", "zhir-models"],
        ),
        ("zhir-tools", vec!["zhir-core", "zhir-policies"]),
        (
            "zhir-builtins",
            vec!["zhir-core", "zhir-tools", "zhir-kernel"],
        ),
        (
            "zhir-testing",
            vec![
                "zhir-core",
                "zhir-kernel",
                "zhir-models",
                "zhir-policies",
                "zhir-minimax",
                "zhir-openai",
            ],
        ),
        (
            "zhir",
            vec![
                "zhir-core",
                "zhir-policies",
                "zhir-kernel",
                "zhir-models",
                "zhir-tools",
                "zhir-storage",
                "zhir-builtins",
            ],
        ),
    ]
    .into_iter()
    .collect();
    let mut checked = 0;
    for package in metadata["packages"].as_array().unwrap() {
        let name = package["name"].as_str().unwrap();
        let Some(allowed) = expected.get(name) else {
            assert_eq!(
                package["publish"],
                serde_json::json!([]),
                "untracked production package: {name}"
            );
            continue;
        };
        if name == "zhir-testing" {
            assert_eq!(package["publish"], serde_json::json!([]));
        } else {
            assert_ne!(package["publish"], serde_json::json!([]));
            for dependency in package["dependencies"].as_array().unwrap() {
                assert_ne!(
                    dependency["name"], "zhir-testing",
                    "test dependency in {name}"
                );
                assert_ne!(
                    dependency["name"], "zhir-conformance",
                    "conformance dependency in {name}"
                );
            }
        }
        let actual: BTreeSet<_> = package["dependencies"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|d| d["kind"] != "dev")
            .map(|d| d["name"].as_str().unwrap())
            .filter(|n| n.starts_with("zhir"))
            .collect();
        assert_eq!(actual, allowed.iter().copied().collect(), "{name}");
        if name == "zhir-builtins" {
            let features = &package["features"];
            assert_eq!(features["agent"], serde_json::json!(["dep:schemars"]));
            assert_eq!(
                features["agent-runtime"],
                serde_json::json!(["agent", "dep:zhir-kernel"])
            );
        }
        if name == "zhir-models" {
            let features = &package["features"];
            assert_eq!(
                features["webrtc"],
                serde_json::json!(["dep:webrtc", "dep:bytes"])
            );
            assert_eq!(
                features["websocket"],
                serde_json::json!(["dep:tokio-tungstenite", "dep:futures"])
            );
            let names: BTreeSet<_> = features
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect();
            assert_eq!(
                names,
                [
                    "default",
                    "openai-chat",
                    "openai-responses",
                    "anthropic",
                    "websocket",
                    "webrtc"
                ]
                .into()
            );
        }
        if matches!(name, "zhir-minimax" | "zhir-openai") {
            let models = package["dependencies"]
                .as_array()
                .unwrap()
                .iter()
                .find(|dependency| {
                    dependency["name"] == "zhir-models" && dependency["kind"].is_null()
                })
                .unwrap();
            let transport = if name == "zhir-minimax" {
                "websocket"
            } else {
                "webrtc"
            };
            assert_eq!(models["features"], serde_json::json!([transport]));
            assert_eq!(models["uses_default_features"], false);
        }
        checked += 1;
    }
    assert_eq!(checked, expected.len());
}

#[test]
fn acceptance_code_is_confined_to_testing_modules() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../crates");
    for name in [
        "zhir",
        "zhir-core",
        "zhir-kernel",
        "zhir-policies",
        "zhir-models",
        "zhir-minimax",
        "zhir-openai",
        "zhir-tools",
        "zhir-storage",
        "zhir-builtins",
    ] {
        let package = root.join(name);
        for directory in ["tests", "benches"] {
            let path = package.join(directory);
            assert!(
                !path.exists() || std::fs::read_dir(&path).unwrap().next().is_none(),
                "acceptance directory in {name}: {directory}"
            );
        }
        let mut pending = vec![package.join("src")];
        while let Some(directory) = pending.pop() {
            for entry in std::fs::read_dir(directory).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    pending.push(path);
                } else if path.extension().is_some_and(|extension| extension == "rs") {
                    let source = std::fs::read_to_string(&path).unwrap();
                    for marker in ["#[test]", "#[tokio::test", "#[cfg(test)]"] {
                        assert!(!source.contains(marker), "test code in {}", path.display());
                    }
                }
            }
        }
    }
}
