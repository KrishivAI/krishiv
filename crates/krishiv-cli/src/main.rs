#![forbid(unsafe_code)]

use std::env;
use std::process;

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let response = krishiv_cli::dispatch(&arg_refs);

    if !response.stdout.is_empty() {
        print!("{}", response.stdout);
    }

    if !response.stderr.is_empty() {
        eprint!("{}", response.stderr);
    }

    process::exit(response.exit_code);
}
