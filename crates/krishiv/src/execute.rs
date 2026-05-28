use krishiv_api::QueryResult;

use crate::relation::Relation;
use crate::stream_handle::StreamHandle;

/// Trait providing unified execution entry points for [`Relation`].
///
/// Implemented for [`Relation`]; import this trait to call `.collect()`,
/// `.explain()`, and `.sink_to()` on a relation in generic contexts.
pub trait Execute: Sized {
    /// Collect results into a [`QueryResult`].
    fn collect(self) -> crate::Result<QueryResult>;

    /// Return a human-readable execution plan description.
    fn explain(&self) -> crate::Result<String>;

    /// Write results to `sink` and return a [`StreamHandle`].
    fn sink_to(
        self,
        sink: impl krishiv_connectors::DynSink + 'static,
    ) -> crate::Result<StreamHandle>;
}

impl Execute for Relation {
    fn collect(self) -> crate::Result<QueryResult> {
        Relation::collect(self)
    }

    fn explain(&self) -> crate::Result<String> {
        Relation::explain(self)
    }

    fn sink_to(
        self,
        sink: impl krishiv_connectors::DynSink + 'static,
    ) -> crate::Result<StreamHandle> {
        Relation::sink_to(self, sink)
    }
}
