//! Barrier injection tests (R16 S1.2).

use krishiv_dataflow::queue::{OperatorMessage, operator_queue};
use krishiv_executor::barrier_transport::{BarrierInjector, make_checkpoint_barrier};

#[test]
fn source_emits_barrier_after_data_via_queue() {
    let (tx, mut rx) = operator_queue(4);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        tx.send_data(arrow::record_batch::RecordBatch::new_empty(
            std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        ))
        .await
        .unwrap();
    });
    let first = rt.block_on(rx.recv()).unwrap();
    assert!(matches!(first, OperatorMessage::Data(_)));
    rt.block_on(async {
        tx.send_barrier(1).await.unwrap();
    });
    let second = rt.block_on(rx.recv()).unwrap();
    assert!(matches!(second, OperatorMessage::Barrier { epoch: 1 }));
}

#[test]
fn barrier_injector_enforces_monotonic_epochs() {
    let mut inj = BarrierInjector::new();
    inj.enqueue(make_checkpoint_barrier("job", 1, "cp"));
    assert_eq!(inj.next_barrier().unwrap().epoch, 1);
    inj.enqueue(make_checkpoint_barrier("job", 1, "cp-dup"));
    assert!(inj.next_barrier().is_none());
}
