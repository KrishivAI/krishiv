"""Exhaustively construct every public F.* function so each definition is
exercised (maximises krishiv.sql.functions coverage and guards the surface)."""
import inspect

import pytest

from krishiv import col, lit
from krishiv.sql import functions as F
from krishiv._pyspark import _Explode


def _identity_lambda(*args):
    # Works for 1-arg (transform/filter/exists/forall), 2-arg (zip_with/
    # aggregate merge) higher-order lambdas — returns the element/accumulator.
    return args[0]


def _arg_for(param):
    """Pick a valid dummy argument for a parameter from its annotation/name."""
    annotation = param.annotation
    text = annotation if isinstance(annotation, str) else getattr(annotation, "__name__", str(annotation))
    name = param.name.lower()
    if "Callable" in text:
        return _identity_lambda
    if name in ("data_type", "returntype"):
        return "int"
    if text == "int" or (text.endswith("int") and "Column" not in text):
        return 1
    if text == "str" or (text == "str" or (text.endswith("str") and "Column" not in text)):
        return "x"
    # ColumnLike / ColumnOrName / Column / Any / everything else
    return col("a")


def _call(fn):
    sig = inspect.signature(fn)
    args = []
    for param in sig.parameters.values():
        if param.kind == param.VAR_POSITIONAL:
            args.append(col("a"))  # one vararg
            continue
        if param.default is not inspect.Parameter.empty:
            continue  # optional — omit
        if param.kind == param.VAR_KEYWORD:
            continue
        args.append(_arg_for(param))
    return fn(*args)


_ALL = [n for n in F.__all__ if callable(getattr(F, n, None)) and not n.startswith("_")]


@pytest.mark.parametrize("name", _ALL)
def test_every_function_constructs(name):
    from krishiv import Column  # noqa: PLC0415

    result = _call(getattr(F, name))
    # explode/posexplode return a generator marker; everything else a Column.
    assert isinstance(result, (Column, _Explode)), f"{name} returned {type(result)!r}"


def test_function_manifest_is_exhaustive():
    # Sanity: we actually enumerated a large surface.
    assert len(_ALL) >= 150


def test_when_chain_and_otherwise():
    c = F.when(col("x") > lit(0), "pos").when(col("x") < lit(0), "neg").otherwise("zero")
    sql = c.sql()
    assert sql.startswith("CASE WHEN") and sql.endswith("END") and "ELSE" in sql


def test_count_variants():
    assert F.count().sql() == F.count_all().sql()
    assert F.count("x").sql() != F.count_all().sql()


def test_literal_and_flatten_paths():
    # nvl2 routes its literal branches through _to_column -> _lit
    assert "CASE WHEN" in F.nvl2(col("a"), 1, 2).sql()
    # greatest/least accept a single list (flatten) as well as varargs
    assert "greatest" in F.greatest([col("a"), col("b")]).sql().lower()
    assert "least" in F.least(col("a"), col("b")).sql().lower()


def test_argument_validation_errors():
    with pytest.raises(TypeError):
        F.when("not a column", 1)
    with pytest.raises(ValueError):
        F.greatest()
    with pytest.raises(ValueError):
        F.least()
    with pytest.raises(ValueError):
        F.count_distinct()


def test_locate_and_substr_variants():
    # locate with a non-default position takes the offset branch
    assert "strpos" in F.locate("x", col("a"), pos=3).sql().lower()
    # substr with an explicit length argument
    assert "substr" in F.substr(col("a"), lit(1), lit(2)).sql().lower()
    assert "substr" in F.substr(col("a"), lit(1)).sql().lower()


def test_hof_arity_signature_paths():
    # a normal lambda's positional arity is introspected directly
    assert F._hof_arity(lambda x: x) == 1
    assert F._hof_arity(lambda x, i: x) == 2
    # a builtin without an inspectable signature falls back to single-arg
    assert F._hof_arity(max) == 1
