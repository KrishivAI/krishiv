#![forbid(unsafe_code)]

use krishiv_cli::dispatch;

#[test]
fn sql_literal_matches_golden_output() {
    let response = dispatch(&["sql", "--query", "select 1 as value"]);

    assert_eq!(response.exit_code, 0, "{}", response.stderr);
    assert_eq!(
        normalize(&response.stdout),
        include_str!("../../../tests/golden/r1-sql-literal.txt")
    );
}

#[test]
fn explain_literal_matches_golden_output() {
    let response = dispatch(&["explain", "--query", "select 1 as value"]);

    assert_eq!(response.exit_code, 0, "{}", response.stderr);
    assert_eq!(
        normalize(&response.stdout),
        include_str!("../../../tests/golden/r1-explain-literal.txt")
    );
}

fn normalize(value: &str) -> String {
    value.replace("\r\n", "\n")
}
