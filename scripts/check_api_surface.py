#!/usr/bin/env python3
"""Generate and validate public API inventories, reports, and Python stubs."""

from __future__ import annotations

import argparse
import json
import re
import sys
import tomllib
from pathlib import Path

VALID_PHASE_STATUS = {"todo", "in_progress", "implemented", "blocked"}
VALID_CAPABILITY_STATUS = {"todo", "partial", "implemented", "blocked", "not_applicable"}
VALID_STABILITY = {"stable", "preview", "experimental", "internal"}
PYCLASS_RE = re.compile(r'#\[pyclass(?:\(([^]]*)\))?\]\s*\npub struct\s+(\w+)', re.MULTILINE)
PYCLASS_NAME_RE = re.compile(r'name\s*=\s*"([^"]+)"')
PYMETHOD_RE = re.compile(
    r'^\s*(?:pub\s+)?(?P<async>async\s+)?fn\s+(?P<name>[A-Za-z_]\w*)(?:<[^>]*>)?\s*\((?P<args>[^)]*)\)',
    re.MULTILINE,
)
PUBLIC_SIGNATURE_RE = re.compile(
    r'(?m)^(?P<attrs>(?:#\[[^\n]+\]\n)*)\s*pub\s+(?P<async>async\s+)?'
    r'(?P<kind>struct|enum|trait|type|fn)\s+(?P<name>\w+)(?P<tail>[^\n{;]*)'
)
PUBLIC_METHOD_RE = re.compile(
    r'(?m)^(?P<attrs>(?:\s*#\[[^\n]+\]\n)*)\s*pub\s+(?P<async>async\s+)?fn\s+'
    r'(?P<name>\w+)\s*\((?P<args>[^)]*)\)(?P<ret>\s*->\s*[^\n{]+)?'
)
PUBLIC_USE_RE = re.compile(r'^pub use\s+([^;]+);', re.MULTILINE)
PYFUNCTION_REG_RE = re.compile(r'wrap_pyfunction!\((?:\w+::)?(\w+),\s*m\)')
DEPRECATED_NOTE_RE = re.compile(r'deprecated\s*\(\s*note\s*=\s*"([^"]+)"')


def source_files(root: Path, relative: str) -> list[Path]:
    base = root / relative
    return sorted(base.rglob("*.rs")) if base.exists() else []


def line_number(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def metadata(path: Path, root: Path, text: str, match: re.Match[str], name: str, kind: str) -> dict[str, object]:
    attrs = match.groupdict().get("attrs", "") or ""
    note = DEPRECATED_NOTE_RE.search(attrs)
    line = line_number(text, match.start())
    return {
        "id": f"{path.relative_to(root)}::{name}",
        "name": name,
        "kind": kind,
        "stability": "preview",
        "documentation": f"{path.relative_to(root)}#L{line}",
        "deprecated": note is not None,
        "replacement": note.group(1) if note else None,
    }


def matching_brace(text: str, opening: int) -> int:
    depth = 0
    for index in range(opening, len(text)):
        if text[index] == "{":
            depth += 1
        elif text[index] == "}":
            depth -= 1
            if depth == 0:
                return index
    return len(text)


def python_inventory(root: Path) -> dict[str, object]:
    classes: list[dict[str, object]] = []
    seen: dict[str, Path] = {}
    duplicates: list[str] = []
    rust_to_public: dict[str, str] = {}
    texts: dict[Path, str] = {}
    for path in source_files(root, "crates/krishiv-python/src"):
        text = path.read_text(encoding="utf-8")
        texts[path] = text
        for match in PYCLASS_RE.finditer(text):
            args, rust_name = match.groups()
            name_match = PYCLASS_NAME_RE.search(args or "")
            public_name = name_match.group(1) if name_match else rust_name
            rust_to_public[rust_name] = public_name
            if public_name in seen:
                duplicates.append(
                    f"Python class {public_name!r} is declared in both "
                    f"{seen[public_name].relative_to(root)} and {path.relative_to(root)}"
                )
            else:
                seen[public_name] = path
            item = metadata(path, root, text, match, public_name, "class")
            item["rust_type"] = rust_name
            item["methods"] = []
            classes.append(item)
    by_rust = {item["rust_type"]: item for item in classes}
    impl_re = re.compile(r'#\[pymethods\]\s*impl\s+(\w+)\s*\{', re.MULTILINE)
    for path, text in texts.items():
        for impl_match in impl_re.finditer(text):
            rust_name = impl_match.group(1)
            if rust_name not in by_rust:
                continue
            opening = text.find("{", impl_match.start())
            body_end = matching_brace(text, opening)
            body = text[opening + 1:body_end]
            base_offset = opening + 1
            methods = []
            for method_match in PYMETHOD_RE.finditer(body):
                name = method_match.group("name")
                absolute_start = base_offset + method_match.start()
                line = line_number(text, absolute_start)
                methods.append({
                    "id": f"{by_rust[rust_name]['name']}.{name}",
                    "name": name,
                    "async": bool(method_match.group("async")),
                    "kind": "method",
                    "stability": "preview",
                    "documentation": f"{path.relative_to(root)}#L{line}",
                    "deprecated": False,
                    "replacement": None,
                })
            by_rust[rust_name]["methods"] = sorted(methods, key=lambda item: item["name"])
    functions: list[dict[str, object]] = []
    lib = texts.get(root / "crates/krishiv-python/src/lib.rs", "")
    for name in sorted(set(PYFUNCTION_REG_RE.findall(lib))):
        functions.append({
            "id": name,
            "name": name,
            "kind": "function",
            "stability": "preview",
            "documentation": "crates/krishiv-python/src/lib.rs",
            "deprecated": False,
            "replacement": None,
        })
    return {"classes": sorted(classes, key=lambda item: item["name"]), "functions": functions, "duplicates": duplicates}


def unique_ids(items: list[dict[str, object]]) -> list[dict[str, object]]:
    counts: dict[str, int] = {}
    for item in items:
        base = str(item["id"])
        counts[base] = counts.get(base, 0) + 1
        if counts[base] > 1:
            item["id"] = f"{base}#{counts[base]}"
    return items

def rust_inventory(root: Path) -> dict[str, object]:
    files = source_files(root, "crates/krishiv-api/src") + source_files(root, "crates/krishiv/src")
    items: list[dict[str, object]] = []
    signatures: list[str] = []
    reexports: set[str] = set()
    for path in files:
        text = path.read_text(encoding="utf-8")
        for match in PUBLIC_SIGNATURE_RE.finditer(text):
            name, kind = match.group("name"), match.group("kind")
            item = metadata(path, root, text, match, name, kind)
            signature = " ".join(match.group(0).split())
            item["signature"] = signature
            item["id"] = f"{path.relative_to(root)}::{kind}::{name}::{signature}"
            items.append(item)
            signatures.append(f"{item['id']}")
        reexports.update(" ".join(value.split()) for value in PUBLIC_USE_RE.findall(text))
    items = unique_ids(items)
    return {
        "items": sorted(items, key=lambda item: item["id"]),
        "reexports": sorted(reexports),
        "report": sorted(str(item["id"]) for item in items),
    }


def sql_inventory(root: Path) -> dict[str, object]:
    items: list[dict[str, object]] = []
    modules: set[str] = set()
    for path in source_files(root, "crates/krishiv-sql/src"):
        text = path.read_text(encoding="utf-8")
        modules.add(str(path.relative_to(root / "crates/krishiv-sql/src")).removesuffix(".rs"))
        for match in PUBLIC_SIGNATURE_RE.finditer(text):
            name, kind = match.group("name"), match.group("kind")
            item = metadata(path, root, text, match, name, kind)
            signature = " ".join(match.group(0).split())
            item["signature"] = signature
            item["id"] = f"{path.relative_to(root)}::{kind}::{name}::{signature}"
            items.append(item)
    items = unique_ids(items)
    return {"items": sorted(items, key=lambda item: item["id"]), "modules": sorted(modules)}


def validate_manifest(root: Path) -> list[str]:
    data = tomllib.loads((root / "api/stable-api.toml").read_text(encoding="utf-8"))
    failures: list[str] = []
    phases = data.get("phases", [])
    phase_ids = [phase.get("id") for phase in phases]
    expected = list("ABCDEFGHI")
    if phase_ids != expected:
        failures.append(f"phases must be declared exactly as {expected}, got {phase_ids}")
    for phase in phases:
        if phase.get("status") not in VALID_PHASE_STATUS:
            failures.append(f"phase {phase.get('id')} has invalid status {phase.get('status')!r}")
        if not phase.get("exit_criteria"):
            failures.append(f"phase {phase.get('id')} has no exit criteria")
    capability_ids: set[str] = set()
    for capability in data.get("capabilities", []):
        identifier = capability.get("id")
        if identifier in capability_ids:
            failures.append(f"duplicate capability id {identifier!r}")
        capability_ids.add(identifier)
        if capability.get("phase") not in expected:
            failures.append(f"capability {identifier!r} has unknown phase")
        if capability.get("stability") not in VALID_STABILITY:
            failures.append(f"capability {identifier!r} has invalid stability")
        for language in ("rust", "python", "sql"):
            if capability.get(language) not in VALID_CAPABILITY_STATUS:
                failures.append(f"capability {identifier!r} has invalid {language} status {capability.get(language)!r}")
        if not capability.get("owner"):
            failures.append(f"capability {identifier!r} has no owner")
    return failures


STUB_HEADER = '''"""Generated preview type surface for the native ``krishiv`` module."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator, Sequence
from typing import Literal, TypeAlias

ColumnLike: TypeAlias = "Column | str | None | bool | int | float | bytes"
ColumnOrName: TypeAlias = "Column | str"
DataFormat: TypeAlias = Literal["parquet", "csv", "json", "ndjson"]
ExplainMode: TypeAlias = Literal["logical", "physical", "analyze"]
JoinType: TypeAlias = Literal[
    "inner",
    "left",
    "right",
    "full",
    "left_semi",
    "right_semi",
    "left_anti",
    "right_anti",
]
QueryStatusName: TypeAlias = Literal[
    "pending",
    "running",
    "completed",
    "cancelled",
    "failed",
    "consumed",
]
RuntimeMode: TypeAlias = Literal["embedded", "local", "distributed"]
WriteMode: TypeAlias = Literal[
    "error",
    "error_if_exists",
    "append",
    "overwrite",
    "ignore",
    "dynamic_overwrite",
]
'''


CLASS_METHOD_SIGNATURES: dict[tuple[str, str], list[str]] = {
    ("Batch", "new"): ["    def __init__(self, obj: object) -> None: ..."],
    ("Batch", "num_rows"): ["    @property", "    def num_rows(self) -> int: ..."],
    ("Batch", "num_columns"): ["    @property", "    def num_columns(self) -> int: ..."],
    ("Batch", "to_arrow"): ["    def to_arrow(self) -> object: ..."],
    ("Batch", "to_pandas"): ["    def to_pandas(self) -> object: ..."],
    ("BlockingSession", "embedded"): ["    @staticmethod", "    def embedded() -> BlockingSession: ..."],
    ("BlockingSession", "from_env"): ["    @staticmethod", "    def from_env() -> BlockingSession: ..."],
    ("BlockingSession", "connect"): ["    @staticmethod", "    def connect(coordinator_url: str) -> BlockingSession: ..."],
    ("BlockingSession", "sql"): ["    def sql(self, query: str) -> QueryResult: ..."],
    ("BlockingSession", "collect"): ["    def collect(self, dataframe: DataFrame) -> QueryResult: ..."],
    ("Column", "alias"): ["    def alias(self, name: str) -> Column: ..."],
    ("Column", "asc"): ["    def asc(self) -> Column: ..."],
    ("Column", "cast"): ["    def cast(self, data_type: str) -> Column: ..."],
    ("Column", "desc"): ["    def desc(self) -> Column: ..."],
    ("Column", "is_null"): ["    def is_null(self) -> Column: ..."],
    ("Column", "is_not_null"): ["    def is_not_null(self) -> Column: ..."],
    ("Column", "normalized_ast"): ["    def normalized_ast(self) -> str: ..."],
    ("Column", "over"): [
        "    def over(",
        "        self, partition_by: Sequence[Column] = ..., order_by: Sequence[Column] = ...",
        "    ) -> Column: ...",
    ],
    ("Column", "sql"): ["    def sql(self) -> str: ..."],
    ("Column", "try_cast"): ["    def try_cast(self, data_type: str) -> Column: ..."],
    ("Column", "__add__"): ["    def __add__(self, other: ColumnLike) -> Column: ..."],
    ("Column", "__and__"): ["    def __and__(self, other: ColumnLike) -> Column: ..."],
    ("Column", "__bool__"): ["    def __bool__(self) -> bool: ..."],
    ("Column", "__mul__"): ["    def __mul__(self, other: ColumnLike) -> Column: ..."],
    ("Column", "__or__"): ["    def __or__(self, other: ColumnLike) -> Column: ..."],
    ("Column", "__sub__"): ["    def __sub__(self, other: ColumnLike) -> Column: ..."],
    ("Column", "__truediv__"): ["    def __truediv__(self, other: ColumnLike) -> Column: ..."],
    ("DataFrame", "alias"): ["    def alias(self, name: str) -> DataFrame: ..."],
    ("DataFrame", "boundedness"): ["    def boundedness(self) -> Literal['bounded', 'unbounded']: ..."],
    ("DataFrame", "cache"): ["    def cache(self) -> DataFrame: ..."],
    ("DataFrame", "collect"): ["    def collect(self) -> QueryResult: ..."],
    ("DataFrame", "collect_async"): ["    async def collect_async(self) -> QueryResult: ..."],
    ("DataFrame", "collect_batches"): ["    def collect_batches(self) -> QueryResult: ..."],
    ("DataFrame", "collect_pretty"): ["    def collect_pretty(self) -> str: ..."],
    ("DataFrame", "collect_with_stats"): ["    def collect_with_stats(self) -> tuple[QueryResult, dict[str, int]]: ..."],
    ("DataFrame", "columns"): ["    def columns(self) -> list[str]: ..."],
    ("DataFrame", "create_or_replace_temp_view"): ["    def create_or_replace_temp_view(self, name: str) -> None: ..."],
    ("DataFrame", "describe"): ["    def describe(self) -> DataFrame: ..."],
    ("DataFrame", "distinct"): ["    def distinct(self) -> DataFrame: ..."],
    ("DataFrame", "drop_columns"): ["    def drop_columns(self, columns: Sequence[str]) -> DataFrame: ..."],
    ("DataFrame", "drop_nulls"): ["    def drop_nulls(self, columns: Sequence[str] = ...) -> DataFrame: ..."],
    ("DataFrame", "except_"): ["    def except_(self, right: DataFrame) -> DataFrame: ..."],
    ("DataFrame", "except_all"): ["    def except_all(self, right: DataFrame) -> DataFrame: ..."],
    ("DataFrame", "except_distinct"): ["    def except_distinct(self, right: DataFrame) -> DataFrame: ..."],
    ("DataFrame", "execute_stream_async"): ["    async def execute_stream_async(self) -> DataFrameStream: ..."],
    ("DataFrame", "explain"): ["    def explain(self) -> str: ..."],
    ("DataFrame", "explain_logical"): ["    def explain_logical(self) -> str: ..."],
    ("DataFrame", "explain_mode"): ["    def explain_mode(self, mode: ExplainMode = ...) -> str: ..."],
    ("DataFrame", "fill_null"): ["    def fill_null(self, column: str, value: str) -> DataFrame: ..."],
    ("DataFrame", "filter"): ["    def filter(self, predicate: str) -> DataFrame: ..."],
    ("DataFrame", "filter_column"): ["    def filter_column(self, predicate: Column) -> DataFrame: ..."],
    ("DataFrame", "group_by"): ["    def group_by(self, expressions: Sequence[str]) -> GroupedDataFrame: ..."],
    ("DataFrame", "group_by_columns"): ["    def group_by_columns(self, expressions: Sequence[Column]) -> GroupedDataFrame: ..."],
    ("DataFrame", "intersect"): ["    def intersect(self, right: DataFrame) -> DataFrame: ..."],
    ("DataFrame", "intersect_distinct"): ["    def intersect_distinct(self, right: DataFrame) -> DataFrame: ..."],
    ("DataFrame", "is_bounded"): ["    def is_bounded(self) -> bool: ..."],
    ("DataFrame", "join"): ["    def join(self, right: DataFrame, on: Sequence[str], *, how: JoinType = ...) -> DataFrame: ..."],
    ("DataFrame", "join_on"): [
        "    def join_on(",
        "        self, right: DataFrame, left_on: Sequence[str], right_on: Sequence[str], *, how: JoinType = ...",
        "    ) -> DataFrame: ...",
    ],
    ("DataFrame", "limit"): ["    def limit(self, n: int) -> DataFrame: ..."],
    ("DataFrame", "num_rows"): ["    def num_rows(self) -> int: ..."],
    ("DataFrame", "order_by"): ["    def order_by(self, columns: Sequence[str]) -> DataFrame: ..."],
    ("DataFrame", "persist"): ["    def persist(self) -> DataFrame: ..."],
    ("DataFrame", "pivot"): [
        "    def pivot(",
        "        self,",
        "        groups: Sequence[Column],",
        "        pivot_column: Column,",
        "        aggregate: Column,",
        "        values: Sequence[tuple[Column, str]],",
        "    ) -> DataFrame: ...",
    ],
    ("DataFrame", "rename"): ["    def rename(self, old: str, new: str) -> DataFrame: ..."],
    ("DataFrame", "repartition"): ["    def repartition(self, num_partitions: int, key_columns: Sequence[str] = ...) -> DataFrame: ..."],
    ("DataFrame", "sample"): ["    def sample(self, fraction: float) -> DataFrame: ..."],
    ("DataFrame", "schema"): ["    def schema(self) -> list[tuple[str, str]]: ..."],
    ("DataFrame", "select"): ["    def select(self, columns: Sequence[str]) -> DataFrame: ..."],
    ("DataFrame", "select_columns"): ["    def select_columns(self, expressions: Sequence[Column]) -> DataFrame: ..."],
    ("DataFrame", "select_exprs"): ["    def select_exprs(self, expressions: Sequence[str]) -> DataFrame: ..."],
    ("DataFrame", "show"): ["    def show(self, n: int = ...) -> None: ..."],
    ("DataFrame", "sort"): ["    def sort(self, columns: Sequence[str], descending: Sequence[bool] | None = ...) -> DataFrame: ..."],
    ("DataFrame", "to_streaming"): ["    def to_streaming(self) -> StreamingDataFrame: ..."],
    ("DataFrame", "union"): ["    def union(self, right: DataFrame) -> DataFrame: ..."],
    ("DataFrame", "union_distinct"): ["    def union_distinct(self, right: DataFrame) -> DataFrame: ..."],
    ("DataFrame", "unpersist"): ["    def unpersist(self) -> None: ..."],
    ("DataFrame", "unpivot"): ["    def unpivot(self, columns: Sequence[str], name_column: str, value_column: str) -> DataFrame: ..."],
    ("DataFrame", "with_column"): ["    def with_column(self, name: str, expression: str) -> DataFrame: ..."],
    ("DataFrame", "write_csv"): ["    def write_csv(self, path: str) -> None: ..."],
    ("DataFrame", "write_csv_with_options"): ["    def write_csv_with_options(self, path: str, *, delimiter: str | None = ..., has_header: bool | None = ...) -> None: ..."],
    ("DataFrame", "write_file"): ["    def write_file(self, path: str, format: DataFormat, *, mode: WriteMode = ..., partition_by: Sequence[str] = ..., max_rows_per_file: int | None = ...) -> None: ..."],
    ("DataFrame", "write_json"): ["    def write_json(self, path: str) -> None: ..."],
    ("DataFrame", "write_parquet"): ["    def write_parquet(self, path: str) -> None: ..."],
    ("DataFrame", "write_parquet_with_options"): ["    def write_parquet_with_options(self, path: str, *, compression: str | None = ..., max_row_group_size: int | None = ...) -> None: ..."],
    ("DataFrame", "write_stream"): ["    def write_stream(self) -> DataStreamWriter: ..."],
    ("DataFrameStream", "__aiter__"): ["    def __aiter__(self) -> AsyncIterator[Batch]: ..."],
    ("DataFrameStream", "__anext__"): ["    async def __anext__(self) -> Batch: ..."],
    ("GroupedDataFrame", "agg"): ["    def agg(self, expressions: Sequence[str]) -> DataFrame: ..."],
    ("GroupedDataFrame", "agg_columns"): ["    def agg_columns(self, expressions: Sequence[Column]) -> DataFrame: ..."],
    ("GroupedDataFrame", "agg_grouping_sets"): ["    def agg_grouping_sets(self, sets: Sequence[Sequence[Column]], aggregates: Sequence[Column]) -> DataFrame: ..."],
    ("GroupedDataFrame", "count"): ["    def count(self) -> DataFrame: ..."],
    ("GroupedDataFrame", "cube"): ["    def cube(self, groups: Sequence[Column], aggregates: Sequence[Column]) -> DataFrame: ..."],
    ("GroupedDataFrame", "rollup"): ["    def rollup(self, groups: Sequence[Column], aggregates: Sequence[Column]) -> DataFrame: ..."],
    ("QueryHandle", "cancel"): ["    def cancel(self) -> None: ..."],
    ("QueryHandle", "collect"): ["    def collect(self) -> QueryResult: ..."],
    ("QueryHandle", "collect_async"): ["    async def collect_async(self) -> QueryResult: ..."],
    ("QueryHandle", "is_done"): ["    def is_done(self) -> bool: ..."],
    ("QueryHandle", "progress"): ["    def progress(self) -> tuple[int, int]: ..."],
    ("QueryHandle", "query_id"): ["    def query_id(self) -> int | None: ..."],
    ("QueryHandle", "status"): ["    def status(self) -> QueryStatusName: ..."],
    ("QueryResult", "batches"): ["    def batches(self) -> list[Batch]: ..."],
    ("QueryResult", "pretty"): ["    def pretty(self) -> str: ..."],
    ("QueryResult", "row_count"): ["    @property", "    def row_count(self) -> int: ..."],
    ("QueryResult", "show"): ["    def show(self, n: int = ...) -> None: ..."],
    ("QueryResult", "to_arrow"): ["    def to_arrow(self) -> object: ..."],
    ("QueryResult", "to_pandas"): ["    def to_pandas(self) -> object: ..."],
    ("QueryResult", "__iter__"): ["    def __iter__(self) -> Iterator[Batch]: ..."],
    ("QueryResult", "__len__"): ["    def __len__(self) -> int: ..."],
    ("Schema", "new"): ["    def __init__(self) -> None: ..."],
    ("Schema", "arrow_schema"): ["    @classmethod", "    def arrow_schema(cls) -> object: ..."],
    ("Schema", "column_names"): ["    @classmethod", "    def column_names(cls) -> list[str]: ..."],
    ("Session", "new"): ["    def __init__(self) -> None: ..."],
    ("Session", "embedded"): ["    @classmethod", "    def embedded(cls, *, target_parallelism: int | None = ..., shuffle_partitions: int | None = ..., state_ttl_ms: int | None = ...) -> Session: ..."],
    ("Session", "local"): ["    @classmethod", "    def local(cls, *, target_parallelism: int | None = ..., shuffle_partitions: int | None = ..., state_ttl_ms: int | None = ...) -> Session: ..."],
    ("Session", "connect"): ["    @classmethod", "    def connect(cls, url: str, *, grpc_url: str | None = ..., target_parallelism: int | None = ..., shuffle_partitions: int | None = ..., state_ttl_ms: int | None = ...) -> Session: ..."],
    ("Session", "from_env"): ["    @classmethod", "    def from_env(cls) -> Session: ..."],
    ("Session", "mode"): ["    @property", "    def mode(self) -> RuntimeMode: ..."],
    ("Session", "sql"): ["    def sql(self, query: str) -> DataFrame: ..."],
    ("Session", "sql_async"): ["    async def sql_async(self, query: str) -> DataFrame: ..."],
    ("Session", "submit_async"): ["    def submit_async(self, query: str) -> QueryHandle: ..."],
    ("Session", "table"): ["    def table(self, name: str) -> DataFrame: ..."],
    ("Session", "read_file"): ["    def read_file(self, path: str, format: DataFormat, *, header: bool = ..., delimiter: str = ...) -> DataFrame: ..."],
    ("Session", "read_parquet"): ["    def read_parquet(self, path: str) -> DataFrame: ..."],
    ("Session", "read_csv"): ["    def read_csv(self, path: str) -> DataFrame: ..."],
    ("Session", "read_json"): ["    def read_json(self, path: str) -> DataFrame: ..."],
    ("Session", "read_parquet_with_options"): ["    def read_parquet_with_options(self, path: str, *, batch_size: int | None = ...) -> DataFrame: ..."],
    ("Session", "read_csv_with_options"): ["    def read_csv_with_options(self, path: str, *, delimiter: str | None = ..., has_header: bool | None = ...) -> DataFrame: ..."],
    ("Session", "register_record_batches"): ["    def register_record_batches(self, name: str, batches: Sequence[Batch]) -> None: ..."],
    ("Session", "deregister_table"): ["    def deregister_table(self, name: str) -> None: ..."],
    ("Session", "drop_table"): ["    def drop_table(self, name: str) -> None: ..."],
    ("Session", "table_exists"): ["    def table_exists(self, name: str) -> bool: ..."],
    ("Session", "list_tables"): ["    def list_tables(self) -> list[str]: ..."],
    ("Session", "list_table_identifiers"): ["    def list_table_identifiers(self) -> list[str]: ..."],
    ("Session", "table_metadata"): ["    def table_metadata(self, name: str) -> object: ..."],
    ("Session", "prepare"): ["    def prepare(self, sql: str) -> PreparedStatement: ..."],
    ("Session", "sql_with_timeout"): ["    def sql_with_timeout(self, query: str, timeout_ms: int) -> DataFrame: ..."],
    ("Session", "sql_as"): ["    def sql_as(self, query: str, api_key: str) -> DataFrame: ..."],
    ("Session", "dataframe"): ["    def dataframe(self, batches: Sequence[Batch]) -> DataFrame: ..."],
    ("Session", "set_config"): ["    def set_config(self, key: str, value: str) -> None: ..."],
    ("Session", "get_config"): ["    def get_config(self, key: str) -> str | None: ..."],
    ("Session", "unset_config"): ["    def unset_config(self, key: str) -> None: ..."],
    ("Session", "configs"): ["    def configs(self) -> dict[str, str]: ..."],
    ("Session", "jobs"): ["    def jobs(self) -> list[JobStatus]: ..."],
    ("StreamingDataFrame", "execute_stream_async"): ["    async def execute_stream_async(self) -> DataFrameStream: ..."],
    ("StreamingDataFrame", "key_by"): ["    def key_by(self, column: str) -> StreamingDataFrame: ..."],
    ("StreamingDataFrame", "tumbling_window"): ["    def tumbling_window(self, window_size_ms: int) -> StreamingDataFrame: ..."],
    ("StreamingDataFrame", "sliding_window"): ["    def sliding_window(self, window_size_ms: int, slide_ms: int) -> StreamingDataFrame: ..."],
    ("StreamingDataFrame", "session_window"): ["    def session_window(self, gap_ms: int) -> StreamingDataFrame: ..."],
    ("StreamingDataFrame", "with_event_time"): ["    def with_event_time(self, column: str) -> StreamingDataFrame: ..."],
    ("StreamingDataFrame", "with_watermark_lag"): ["    def with_watermark_lag(self, lag_ms: int) -> StreamingDataFrame: ..."],
    ("DataStreamWriter", "foreach_batch"): ["    def foreach_batch(self, func: object) -> None: ..."],
    ("DataStreamWriter", "option"): ["    def option(self, key: str, value: str) -> None: ..."],
    ("DataStreamWriter", "output_mode"): ["    def output_mode(self, mode: Literal[\"append\", \"update\", \"complete\"]) -> None: ..."],
    ("DataStreamWriter", "query_name"): ["    def query_name(self, name: str) -> None: ..."],
    ("DataStreamWriter", "start"): ["    def start(self) -> StreamingQuery: ..."],
    ("DataStreamWriter", "trigger"): [
        "    def trigger(",
        "        self,",
        "        trigger_type: Literal[\"once\", \"available_now\", \"processing_time\", \"continuous\"],",
        "        interval_ms: int = ...,",
        "    ) -> None: ...",
    ],
    ("Session", "single_node"): [
        "    @classmethod",
        "    def single_node(",
        "        cls,",
        "        url: str,",
        "        *,",
        "        grpc_url: str | None = ...,",
        "        target_parallelism: int | None = ...,",
        "        shuffle_partitions: int | None = ...,",
        "        state_ttl_ms: int | None = ...,",
        "    ) -> Session: ...",
    ],
}


FUNCTION_SIGNATURES: dict[str, str] = {
    "avg": "def avg(column: Column) -> Column: ...",
    "call_function": "def call_function(name: str, arguments: Sequence[Column]) -> Column: ...",
    "col": "def col(name: str) -> Column: ...",
    "count": "def count(column: Column) -> Column: ...",
    "count_all": "def count_all() -> Column: ...",
    "expr": "def expr(sql: str) -> Column: ...",
    "lit": "def lit(value: ColumnLike) -> Column: ...",
    "max": "def max(column: Column) -> Column: ...",
    "min": "def min(column: Column) -> Column: ...",
    "sum": "def sum(column: Column) -> Column: ...",
    "make_example_batch": "def make_example_batch() -> Batch: ...",
    "read_parquet": "def read_parquet(path: str) -> DataFrame: ...",
}


def render_python_stub(inventory: dict[str, object]) -> str:
    lines = STUB_HEADER.splitlines()
    lines.append("")
    for item in inventory["classes"]:
        lines.append(f"class {item['name']}:")
        methods = item.get("methods", [])
        if not methods:
            lines.append("    ...")
        for method in methods:
            name = method["name"]
            override = CLASS_METHOD_SIGNATURES.get((str(item["name"]), str(name)))
            if override:
                lines.extend(override)
            elif name == "new":
                lines.append("    def __init__(self, *args: object, **kwargs: object) -> None: ...")
            else:
                async_prefix = "async " if method.get("async") else ""
                lines.append(f"    {async_prefix}def {name}(self, *args: object, **kwargs: object) -> object: ...")
        lines.append("")
    for function in inventory["functions"]:
        signature = FUNCTION_SIGNATURES.get(
            str(function["name"]),
            f"def {function['name']}(*args: object, **kwargs: object) -> object: ...",
        )
        lines.append(signature)
    return "\n".join(lines).rstrip() + "\n"


def inventories(root: Path) -> tuple[dict[str, object], dict[str, str]]:
    rust = rust_inventory(root)
    python = python_inventory(root)
    sql = sql_inventory(root)
    json_outputs = {
        "rust-public.json": {key: value for key, value in rust.items() if key != "report"},
        "python-public.json": python,
        "sql-public.json": sql,
    }
    text_outputs = {
        "rust-public-api.txt": "\n".join(rust["report"]) + "\n",
        "../crates/krishiv-python/python/krishiv/krishiv.pyi": render_python_stub(python),
    }
    return json_outputs, text_outputs


def encoded(value: object) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    root = args.root.resolve()
    failures = validate_manifest(root)
    generated_json, generated_text = inventories(root)
    failures.extend(generated_json["python-public.json"]["duplicates"])
    for filename, value in generated_json.items():
        destination = root / "api" / filename
        content = encoded(value)
        if args.write:
            destination.write_text(content, encoding="utf-8")
        elif not destination.exists() or destination.read_text(encoding="utf-8") != content:
            failures.append(f"stale or missing API inventory {destination.relative_to(root)}; run with --write")
    for filename, content in generated_text.items():
        destination = (root / "api" / filename).resolve()
        if args.write:
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(content, encoding="utf-8")
        elif not destination.exists() or destination.read_text(encoding="utf-8") != content:
            failures.append(f"stale or missing generated API artifact {destination.relative_to(root)}; run with --write")
    if failures:
        print("API surface validation failed:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1
    print(f"API manifest, inventories, reports, and stubs {'updated' if args.write else 'validated'} successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
