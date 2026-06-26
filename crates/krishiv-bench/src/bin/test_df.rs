// Bench binary intentionally prints to stdout/stderr.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use datafusion::execution::session_state::SessionStateBuilder;
fn main() {
    let state = SessionStateBuilder::new().with_default_features().build();
    let factories = state.table_factories();
    for k in factories.keys() {
        println!("{}", k);
    }
}
