#![forbid(unsafe_code)]

use std::process::Command;

fn run(args: &[&str]) -> (i32, String, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_krishiv"))
        .args(args)
        .output()
        .expect("failed to run krishiv binary");
    (
        out.status.code().unwrap_or(1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn sql_literal_matches_golden_output() {
    let (code, stdout, stderr) = run(&["sql", "--query", "select 1 as value"]);
    assert_eq!(code, 0, "{stderr}");
    assert_eq!(
        normalize(&stdout),
        include_str!("../../../tests/golden/r1-sql-literal.txt")
    );
}

#[test]
fn explain_literal_matches_golden_output() {
    let (code, stdout, stderr) = run(&["explain", "--query", "select 1 as value"]);
    assert_eq!(code, 0, "{stderr}");
    assert_eq!(
        normalize(&stdout),
        include_str!("../../../tests/golden/r1-explain-literal.txt")
    );
}

fn normalize(value: &str) -> String {
    value.replace("\r\n", "\n")
}
