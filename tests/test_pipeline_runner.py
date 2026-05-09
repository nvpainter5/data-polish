"""Tests for the format-detection helpers in api/pipeline_runner.py.

We test the readers + delimiter sniff in isolation so that adding a new
input format (e.g. JSON Lines, pipe-delim with embedded commas) doesn't
require running the full pipeline.
"""

from __future__ import annotations

import sys
from pathlib import Path

# api/ isn't a package on the test runner's PYTHONPATH; add the project
# root so `from api...` works.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from api.pipeline_runner import _smart_read_dataframe, detect_delimiter


def test_detects_pipe_delimiter():
    raw = b"a|b|c\n1|2|3\n4|5|6\n"
    assert detect_delimiter(raw) == "|"


def test_detects_tab_delimiter():
    raw = b"a\tb\tc\n1\t2\t3\n"
    assert detect_delimiter(raw) == "\t"


def test_detects_comma_default():
    raw = b"a,b,c\n1,2,3\n"
    assert detect_delimiter(raw) == ","


def test_detects_semicolon_delimiter():
    raw = b"a;b;c\n1;2;3\n"
    assert detect_delimiter(raw) == ";"


def test_falls_back_to_comma_for_single_column_files():
    raw = b"justonecolumn\nvalue1\nvalue2\n"
    assert detect_delimiter(raw) == ","


def test_smart_read_pipe_delimited():
    """The exact failure mode from the rawCustomer.txt issue."""
    raw = (
        b"CustomerID|Email|Status\n"
        b"42|alice@example.com|ACTIVE\n"
        b"43|bob@example.com|INACTIVE\n"
    )
    df = _smart_read_dataframe(raw)
    assert list(df.columns) == ["CustomerID", "Email", "Status"]
    assert len(df) == 2


def test_smart_read_tab_delimited():
    raw = b"a\tb\tc\n1\t2\t3\n"
    df = _smart_read_dataframe(raw)
    assert list(df.columns) == ["a", "b", "c"]


def test_smart_read_csv():
    raw = b"name,age\nalice,30\nbob,25\n"
    df = _smart_read_dataframe(raw)
    assert list(df.columns) == ["name", "age"]
    assert len(df) == 2


def test_smart_read_json_array():
    raw = b'[{"a": 1, "b": 2}, {"a": 3, "b": 4}]'
    df = _smart_read_dataframe(raw)
    assert list(df.columns) == ["a", "b"]
    assert len(df) == 2


def test_smart_read_json_lines():
    raw = b'{"a": 1, "b": 2}\n{"a": 3, "b": 4}\n'
    df = _smart_read_dataframe(raw)
    assert list(df.columns) == ["a", "b"]
    assert len(df) == 2
