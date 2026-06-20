#[cfg(test)]
mod shuffle_tests {
    use std::collections::HashSet;
    use std::fmt;
    use std::hash::Hasher;
    use std::sync::Arc;

    use arrow::array::{
        Array, Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::local_store::LocalShuffleStore;
    use crate::{
        CompressionCodec, HashPartitioner, InMemoryShuffleStore, LocalDiskShuffleStore,
        PartitionId, PartitionState, ShuffleCompression, ShuffleError,
    };
    use crate::{
        ShuffleMetadata, ShufflePartition, ShufflePath, ShuffleStore, TieredShuffleStore,
        cleanup_orphans, compression::partition_memory_bytes, scan_orphans,
    };

    // ── ShufflePath ───────────────────────────────────────────────────────

    include!("sections/path_metadata.rs.inc");
    include!("sections/local_store.rs.inc");
    include!("sections/compression.rs.inc");
    include!("sections/orphans.rs.inc");
    include!("sections/partitioner.rs.inc");
    include!("sections/store.rs.inc");
    include!("sections/object_store.rs.inc");
    include!("sections/local_disk.rs.inc");
    include!("sections/in_memory.rs.inc");
    include!("sections/compression_ipc.rs.inc");
    include!("sections/metadata_states.rs.inc");
    include!("sections/partitioner_extra.rs.inc");
}
