"""Regression: pass-through multi-day area SQL must bind every %s placeholder."""
from __future__ import annotations

import inspect
import re

from backend.infra import db_access


def _count_placeholders_and_params(fn_name: str) -> tuple[int, int]:
    source = inspect.getsource(getattr(db_access, fn_name))
    m = re.search(r'cur\.execute\(\s*f"""(.*?)"""', source, re.S)
    assert m, f"missing execute block in {fn_name}"
    sql = m.group(1)
    sql = re.sub(r"\{confidence_expr\}", "X", sql)
    sql = re.sub(r"\{note_expr\}", "Y", sql)
    placeholders = len(re.findall(r"(?<!%)%s", sql))
    m2 = re.search(r'""",\s*\((.*?)\),\s*\)\s*return', source, re.S)
    assert m2, f"missing params tuple in {fn_name}"
    params = [p.strip() for p in m2.group(1).split("\n") if p.strip()]
    return placeholders, len(params)


def test_pass_through_multi_day_sql_param_count_matches():
    placeholders, params = _count_placeholders_and_params(
        "_get_routes_in_polygon_pass_through_multi_day"
    )
    assert placeholders == params, (
        f"expected {placeholders} SQL params, got {params} "
        "(psycopg2 raises IndexError: tuple index out of range when mismatched)"
    )
