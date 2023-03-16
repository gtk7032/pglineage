from __future__ import annotations

from pprint import pprint
from typing import Any, Dict, List

from pglast import ast, parse_sql

from parsed import ParsedStatement
from restarget import ResTarget

# sql = "select a, b, c from tbl;"
# sql = "insert into to_table (to_col1, to_col2) \
#   select 5 * from_col1, from_col2 from from_table;"
# sql = "insert into to_table (to_col1, to_col2)"
# sql = "select 5 * from_table1.from_col1, from_table2.from_col4 from from_table inner join from_table2 on from_table.from_col3 = from_table2.from_col4;"

# sql = (
#     "insert into to_table (to_col1, to_col2)"
#     "select case when true then from_table1.from_col1 when false then from_table2.from_col4 else 5 end from from_table inner join from_table2 on from_table.from_col3 = from_table2.from_col4;"
# )

# sql = "INSERT INTO new_table ( col1, col2, col3 ) WITH tmp_table AS ( SELECT col1 as colX, col2, col3, 5 FROM old_table ) SELECT col1, col2, col3, 4 FROM tmp_table"

sql = "SELECT s1.age * s2.age, s1.age_count * 5, 5, 'aa' FROM ( SELECT age, COUNT(age) as age_count FROM students GROUP BY age ) as s1, s2;"
# sql = "UPDATE EMPLOYEES SET SALARY = 8500 WHERE LAST_NAME = 'Keats';"


def parse_select_statement(statement: Dict[str, Any], layer: int) -> ParsedStatement:
    columns: List[ResTarget] = []
    for target in statement["targetList"]:
        column = ResTarget()
        ResTarget.parse_restarget(target, column)
        columns.append(column)

    tables, next = [], []
    for table in statement["fromClause"]:
        if "subquery" in table.keys():
            next.append(parse_select_statement(table["subquery"], layer + 1))
        if "alias" in table.keys():
            tables.append(table["alias"]["aliasname"])
        elif "relname" in table.keys():
            tables.append(table["relname"])

    if len(tables) == 1:
        for col in columns:
            col.attach_table(tables[0])

    return ParsedStatement(layer, columns, tables, next)


if __name__ == "__main__":
    stmt = parse_sql(sql)[0].stmt
    x = stmt(skip_none=True)
    pprint(x)
    if isinstance(stmt, ast.SelectStmt):
        res = parse_select_statement(x, 0)
        res.show()
