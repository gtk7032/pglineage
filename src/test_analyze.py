from typing import Any

from analyzer import Analyzer


def func(sql: str, expected: dict[str, Any]) -> None:
    analyzer = Analyzer()
    analyzer.load([("", sql)])
    nd = analyzer._analyze_test()
    res = nd._flatten().format()
    assert res == expected


def test_subquery_1():
    # muti join, nest
    sql = (
        "SELECT "
        "    tbl1.col1, "
        "    tbl2.col2, "
        "    tbl3.col3 "
        "FROM "
        "    ( "
        "        SELECT "
        "            col1, "
        "            col2 "
        "        FROM "
        "            table1 "
        "    ) AS tbl1 "
        "    INNER JOIN ( "
        "        SELECT "
        "            col1, "
        "            col2, "
        "            col3 "
        "        FROM "
        "            table2 "
        "    ) AS tbl2 ON tbl1.col2 = tbl2.col2 "
        "    INNER JOIN ( "
        "        SELECT "
        "            col1, "
        "            col2, "
        "            col3 "
        "        FROM "
        "            table3 "
        "        WHERE "
        "            EXISTS ( "
        "                SELECT "
        "                    1 "
        "                FROM "
        "                    table3, "
        "                    table4 "
        "                WHERE "
        "                    table3.col4 = table4.col4 "
        "            ) "
        "    ) AS tbl3 ON tbl2.col3 = tbl3.col3; "
    )
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
    func(sql, expected)
