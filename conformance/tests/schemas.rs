#[test]
fn checked_in_schemas_match_native_dtos_and_resolve_offline() {
    let directory =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../contracts/v5/schemas");
    for (name, generated) in zhir_core::wire::schemas() {
        let checked_in: serde_json::Value = serde_json::from_slice(
            &std::fs::read(directory.join(format!("{name}.schema.json"))).unwrap(),
        )
        .unwrap();
        assert_eq!(generated, checked_in, "schema drift: {name}");
        jsonschema::validator_for(&checked_in).unwrap();
    }
}
