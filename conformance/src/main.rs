use std::path::PathBuf;
#[tokio::main]
async fn main() {
    let root = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("cases"));
    let mut paths = std::fs::read_dir(root)
        .expect("case directory")
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|s| s == "json"))
        .collect::<Vec<_>>();
    paths.sort();
    let mut failed = 0;
    let count = paths.len();
    for path in paths {
        let case = zhir_conformance::load(&path).expect("native fixture JSON");
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            zhir_conformance::run_case(&case),
        )
        .await;
        match result {
            Ok(Ok(())) => println!("PASS {}", case["name"].as_str().unwrap()),
            Ok(Err(error)) => {
                failed += 1;
                eprintln!("FAIL {}: {error}", case["name"]);
            }
            Err(_) => {
                failed += 1;
                eprintln!("FAIL {}: case timed out", case["name"]);
            }
        }
    }
    println!("{} passed, {} failed", count - failed, failed);
    if failed > 0 {
        std::process::exit(1);
    }
}
