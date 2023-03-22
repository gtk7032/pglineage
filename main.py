from __future__ import annotations

from pprint import pprint
from typing import Any, Dict, List, Union

from pglast import ast, parse_sql

from field import Field
from parsed import ParsedStatement
from restarget import ResTarget
from table import Table

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

# sql = "SELECT s1.age * s2.age as al, s1.age_count * 5, 5, 'aa' FROM ( SELECT age, COUNT(age) as age_count FROM students as stu GROUP BY age ) as s1, s2;"

sql = (
    "with get_top5_amount_id as ("
    "select customer_id as id, "
    "sum(amount) "
    "from payment "
    ") "
    "select email "
    "from customer, "
    "(select * from tbl ) as tbl2 "
    "join get_top5_amount_id "
    "on customer.customer_id = get_top5_amount_id.id;"
)
# sql = "UPDATE EMPLOYEES SET SALARY = 8500 WHERE LAST_NAME = 'Keats';"


def parse_select_statement(layer: int, statement: Dict[str, Any]) -> ParsedStatement:
    columns: List[ResTarget] = []
    tables = []
    refcolumns: List[List[Field]] = []
    reftables: Dict[str, Union[str, ParsedStatement]] = {}

    for i, target in enumerate(statement["targetList"]):
        column = ResTarget()
        ResTarget.parse_restarget(target, column)
        columns.append(column)
        refcolumns.append(column.fields)

    if "withClause" in statement.keys():
        for cte in statement["withClause"]["ctes"]:
            t = Table("", cte["ctename"])
            # tables.append(t)
            rt = parse_select_statement(layer + 1, cte["ctequery"])
            reftables[str(t)] = rt

    def parse_from_clause(fc):
        if "@" not in fc.keys():
            return

        if fc["@"] == "RangeSubselect":
            t = Table("", fc["alias"]["aliasname"])
            tables.append(t)
            rt = parse_select_statement(layer + 1, fc["subquery"])
            reftables[str(t)] = rt

        elif fc["@"] == "RangeVar":
            table = Table(
                fc["relname"],
                fc["alias"]["aliasname"] if "alias" in fc.keys() else "",
            )
            tables.append(table)
            if table.alias and table.alias not in reftables.keys() and table.entity:
                reftables[table.alias] = table.entity

        for v in fc.values():
            if isinstance(v, Dict):
                parse_from_clause(v)

    if "fromClause" in statement.keys():
        for table in statement["fromClause"]:
            parse_from_clause(table)

    if len(tables) == 1:
        for col in columns:
            col.attach_table(tables[0])

    return ParsedStatement(layer, columns, tables, refcolumns, reftables)


if __name__ == "__main__":
    stmt = parse_sql(sql)[0].stmt
    x = stmt(skip_none=True)
    # pprint(x)
    print("\n")
    if isinstance(stmt, ast.SelectStmt):
        res = parse_select_statement(0, x)
        pprint(res.format())
