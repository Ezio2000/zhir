fn main() {
    let check = std::env::args().any(|arg| arg == "--check");
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../contracts/v3/schemas");
    std::fs::create_dir_all(&root).unwrap();
    for (name, schema) in zhir_core::wire::schemas() {
        let value = format!("{}\n", serde_json::to_string_pretty(&schema).unwrap());
        let path = root.join(format!("{name}.schema.json"));
        if check {
            assert_eq!(
                std::fs::read_to_string(&path).unwrap(),
                value,
                "schema drift: {}",
                path.display()
            );
        } else {
            std::fs::write(path, value).unwrap();
        }
    }
}
