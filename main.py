from pprint import pprint
from typing import Any, Dict, List, Union

from pglast import ast, parse_sql

from column import Column

# sql = "select a, b, c from tbl;"
# sql = "insert into to_table (to_col1, to_col2) \
#   select 5 * from_col1, from_col2 from from_table;"

# sql = "insert into to_table (to_col1, to_col2) \
# sql = "select 5 * from_table1.from_col1, from_table2.from_col4 from from_table inner join from_table2 on from_table.from_col3 = from_table2.from_col4;"

# sql = "insert into to_table (to_col1, to_col2) \
#   select case when true then from_table1.from_col1 when false then from_table2.from_col4 else 5 end from from_table inner join from_table2 on from_table.from_col3 = from_table2.from_col4;"

# sql = "INSERT INTO new_table ( col1, col2, col3 ) WITH tmp_table AS ( SELECT col1 as colX, col2, col3, 5 FROM old_table ) SELECT col1, col2, col3, 4 FROM tmp_table"

sql = "SELECT s1.age, s1.age_count * 5, 5, 'aa' FROM ( SELECT age, COUNT(age) as age_count FROM students GROUP BY age ) as s1, s2;"


def parse_restarget(tgt, result: List[Column]):
    if "@" not in tgt.keys():
        return

    if tgt["@"] == "ColumnRef" and "fields" in tgt.keys():
        col: List[str] = []
        for field in tgt["fields"]:
            if "sval" in field.keys():
                col.append(field["sval"])
        if col:
            result.append(Column.create_from_list(col))
        return

    for val in tgt.values():
        if isinstance(val, dict):
            parse_restarget(val, result)


class Res:
    def __init__(self, layer, columns, tables, next) -> None:
        self.layer = layer
        self.columns = columns
        self.tables = tables
        self.next = next

    def show(self):
        print(f"{self.layer=}")
        for col in self.columns:
            col.show()
        print(self.tables)
        for res in self.next:
            print("\n")
            res.show()


def parse_select_statement(statement: Dict[str, Any], layer) -> Res:
    columns: List[Column] = []
    for target in statement["targetList"]:
        parse_restarget(target, columns)

    tables, next = [], []
    for table in statement["fromClause"]:
        if "subquery" in table.keys():
            next.append(parse_select_statement(table["subquery"], layer + 1))
        if "alias" in table.keys():
            tables.append(table["alias"]["aliasname"])
        elif "relname" in table.keys():
            tables.append(table["relname"])

    if len(tables) == 1:
        Column.add_table(tables[0], columns)

    return Res(layer, columns, tables, next)


if __name__ == "__main__":
    stmt = parse_sql(sql)[0].stmt
    x = stmt(skip_none=True)
    pprint(x)
    if isinstance(stmt, ast.SelectStmt):
        res = parse_select_statement(x, 0)
        res.show()
