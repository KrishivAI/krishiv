//! A bare `collect()` on SQL over an unbounded streaming table must error,
//! not block forever (the live audit found the python surface hanging here).
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use krishiv_api::SessionBuilder;

#[test]
fn collect_on_unbounded_table_errors_instead_of_hanging() {
    let session = SessionBuilder::new().build().expect("session");
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    session.register_unbounded("u_t", schema).expect("register");

    assert!(
        session
            .is_streaming_query("SELECT * FROM u_t")
            .expect("classify"),
        "session must classify the query as streaming"
    );
    let df = session.sql("SELECT * FROM u_t").expect("plan");
    let err = df
        .collect()
        .expect_err("collect() over an unbounded source must error, not block");
    assert!(
        err.to_string().contains("streaming"),
        "error should name the streaming cause: {err}"
    );
}
