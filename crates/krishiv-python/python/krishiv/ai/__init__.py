"""Vector sinks (``krishiv.ai``).

The classes live in the native extension behind the ``vector-sinks`` Cargo
feature (``maturin develop --features vector-sinks``). When that feature is
compiled in, the extension registers itself as ``krishiv.ai`` before this
package is imported and this file is never executed. When it is not, this
package stands in so that ``import krishiv.ai`` still succeeds and any
attribute access explains what is missing instead of failing with a circular
import.

The previous body of this file re-exported chunker and RAG names
(``MarkdownSectionChunker``, ``rag_index``, …) from ``krishiv.ai`` — itself —
which was a self-import; none of those names exist anywhere in the crate.
"""

__all__ = [
    "InMemoryVectorSink",
    "ScoredChunk",
    "LanceDbSink",
    "WeaviateSink",
    "PineconeSink",
    "QdrantSink",
    "PgvectorSink",
]

_FEATURE_HINT = (
    "krishiv.ai.{name} requires the native `vector-sinks` feature: rebuild the "
    "extension with `maturin develop --features vector-sinks` (or install the "
    "`krishiv[ai]` extra)."
)


def __getattr__(name: str):
    # AttributeError (not ImportError) on purpose: `hasattr(krishiv.ai, "X")`
    # is how callers and the test suite probe for the feature, and hasattr
    # only swallows AttributeError. `from krishiv.ai import X` still surfaces
    # as an ImportError, with this message chained as its cause.
    if name in __all__:
        raise AttributeError(_FEATURE_HINT.format(name=name))
    raise AttributeError(f"module 'krishiv.ai' has no attribute {name!r}")
