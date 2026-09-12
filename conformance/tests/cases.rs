#[tokio::test]
async fn all_native_behavior_cases() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("cases");
    let mut paths = std::fs::read_dir(root)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|s| s == "json"))
        .collect::<Vec<_>>();
    paths.sort();
    assert_eq!(paths.len(), 77);
    let mut failures = Vec::new();
    for path in paths {
        let case = zhir_conformance::load(&path).unwrap();
        match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            zhir_conformance::run_case(&case),
        )
        .await
        {
            Ok(Ok(())) => {}
            other => failures.push(format!("{}: {other:?}", case["name"])),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
