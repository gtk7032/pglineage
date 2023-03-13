from pprint import pprint
from typing import Any, Dict, List

from pglast import ast, parse_sql

# sql = "select a, b, c from tbl;"
# sql = "insert into to_table (to_col1, to_col2) \
#   select 5 * from_col1, from_col2 from from_table;"

# sql = "insert into to_table (to_col1, to_col2) \
# sql = "select 5 * from_table1.from_col1, from_table2.from_col4 from from_table inner join from_table2 on from_table.from_col3 = from_table2.from_col4;"

# sql = "insert into to_table (to_col1, to_col2) \
#   select case when true then from_table1.from_col1 when false then from_table2.from_col4 else 5 end from from_table inner join from_table2 on from_table.from_col3 = from_table2.from_col4;"

# sql = "INSERT INTO new_table ( col1, col2, col3 ) WITH tmp_table AS ( SELECT col1 as colX, col2, col3, 5 FROM old_table ) SELECT col1, col2, col3, 4 FROM tmp_table"

sql = "SELECT s1.age, s1.age_count * 5, 5, 'aa' FROM ( SELECT age, COUNT(age) as age_count FROM students GROUP BY age ) as s1;"


def parse_restarget(tgt, result):
    if "@" not in tgt.keys():
        return

    if tgt["@"] == "ColumnRef" and "fields" in tgt.keys():
        fields = []
        for field in tgt["fields"]:
            if "sval" in field.keys():
                fields.append(field["sval"])
        if fields:
            result.append(fields)
        return

    for val in tgt.values():
        if isinstance(val, dict):
            parse_restarget(val, result)


def parse_select_statement(statement: Dict[str, Any]):
    columns: List[List[str]] = []
    for target in statement["targetList"]:
        parse_restarget(target, columns)
    print(columns)
    return


if __name__ == "__main__":
    stmt = parse_sql(sql)[0].stmt
    x = stmt(skip_none=True)
    pprint(x)
    if isinstance(stmt, ast.SelectStmt):
        parse_select_statement(x)
