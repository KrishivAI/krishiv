#![forbid(unsafe_code)]
// CLI entry point intentionally writes errors to stderr.
#![allow(clippy::print_stderr)]

use std::env;
use std::process;

#[tokio::main]
async fn main() {
    match krishiv_executor::cli::run_executor_cli(env::args().skip(1)).await {
        Ok(()) => {}
        Err(error) => {
            eprintln!("{error}");
            process::exit(2);
        }
    }
}
