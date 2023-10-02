from typing import Any

from src.analyzer import Analyzer
from tests import sql


def func(sql: str, expected: dict[str, Any]) -> None:
    analyzer = Analyzer()
    analyzer.load([("", sql)])
    nd = analyzer._analyze_test()
    result = nd._flatten().format()
    assert result == expected


def test_case_subquery_1():
    expected = {
        "refcols": {"col1": [], "col2": [], "col3": []},
        "srccols": {
            "col1": ["table1.col1"],
            "col2": ["table2.col2"],
            "col3": ["table3.col3"],
        },
        "statement": "Select",
        "tables": {
            "table1": "table1",
            "table2": "table2",
            "table3": "table3",
            "table4": "table4",
        },
        "tgttable": "",
    }
    func(sql.case_subquery_1, expected)
