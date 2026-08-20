//! The query-shape axis of the streaming conformance corpus, wired to the
//! compiler it exists to check.
//!
//! `QUERY_SHAPES` lived in krishiv-dataflow with a doc comment claiming "the
//! assertion lives in krishiv-sql" — and no assertion existed anywhere. The
//! corpus that was supposed to force deliberate changes when compiler
//! capabilities moved was enforcing nothing, and its `multi_column_group_by`
//! entry went stale the day composite keys landed. This test is the missing
//! assertion; it lives here (like the other cross-seam tests) because
//! krishiv-sql does not depend on krishiv-dataflow.

use krishiv_dataflow::streaming_corpus::QUERY_SHAPES;
use krishiv_sql::streaming_window_plan::compile_streaming_window_sql;

#[test]
fn every_query_shape_holds_against_the_compiler() {
    let mut failures = Vec::new();
    for shape in QUERY_SHAPES {
        match compile_streaming_window_sql(shape.sql) {
            Ok(plan) => {
                if !shape.compiles {
                    failures.push(format!(
                        "{}: compiled but the corpus says it must be refused — if the \
                         capability landed, flip the corpus entry DELIBERATELY ({})",
                        shape.name, shape.why
                    ));
                    continue;
                }
                if let Some(needle) = shape.spec_must_contain {
                    let debug = format!("{:?}", plan.spec);
                    if !debug.contains(needle) {
                        failures.push(format!(
                            "{}: compiled but the spec lost the field — {:?} not found \
                             in the spec debug form ({})",
                            shape.name, needle, shape.why
                        ));
                    }
                }
            }
            Err(e) => {
                if shape.compiles {
                    failures.push(format!(
                        "{}: must compile but was refused: {e} ({})",
                        shape.name, shape.why
                    ));
                    continue;
                }
                if let Some(needle) = shape.error_must_contain {
                    let msg = e.to_string();
                    if !msg.contains(needle) {
                        failures.push(format!(
                            "{}: refused, but the error is uninformative — {:?} not in \
                             {msg:?} ({})",
                            shape.name, needle, shape.why
                        ));
                    }
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "query shapes out of sync with the compiler:\n  {}",
        failures.join("\n  ")
    );
}
