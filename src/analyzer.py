from typing import Any, Dict, List

from pglast import ast, parse_sql

import node
from restarget import ResTarget
from table import Table


class Analyzer:
    def __init__(self) -> None:
        self.__rawstmts: List[Any] = []

    def load(self, sqls: str) -> None:
        self.__rawstmts = [sql.stmt for sql in parse_sql(sqls)]

    def analyze(self) -> List[node.Select | node.Insert]:
        nodes: List[node.Select | node.Insert] = []
        for rawstmt in self.__rawstmts:
            if isinstance(rawstmt, ast.SelectStmt):
                nodes.append(self.__analyze_select(0, rawstmt(skip_none=True)))
            elif isinstance(rawstmt, ast.InsertStmt):
                nodes.append(self.__analyze_insert(0, rawstmt(skip_none=True)))
        return nodes

    def __analyze_fromclause(
        self, fc: Dict[str, Any], tables: List[Table], layer: int
    ) -> None:
        if "@" not in fc.keys():
            return

        if fc["@"] == "RangeSubselect":
            tables.append(
                Table(
                    fc["alias"]["aliasname"],
                    self.__analyze_select(layer + 1, fc["subquery"]),
                )
            )

        elif fc["@"] == "RangeVar":
            tbl = Table(
                fc["alias"]["aliasname"] if "alias" in fc.keys() else "", fc["relname"]
            )
            if not [t for t in tables if str(t) == str(tbl)]:
                tables.append(tbl)

        for v in fc.values():
            if isinstance(v, Dict):
                self.__analyze_fromclause(v, tables, layer)

    def __analyze_select(self, layer: int, statement: Dict[str, Any]) -> node.Select:
        columns = ResTarget.parse_restarget_list(statement["targetList"])
        tables: List[Table] = []

        if "withClause" in statement.keys():
            for cte in statement["withClause"]["ctes"]:
                tables.append(
                    Table(
                        cte["ctename"],
                        self.__analyze_select(layer + 1, cte["ctequery"]),
                    )
                )

        if "fromClause" in statement.keys():
            for fc in statement["fromClause"]:
                self.__analyze_fromclause(fc, tables, layer)

        if len(tables) == 1:
            for col in columns:
                col.attach_table(tables[0])

        return node.Select(layer, columns, tables)

    def __analyze_insert(self, layer: int, stmt: Dict[str, Any]) -> node.Insert:
        res = ResTarget.parse_restarget_list(stmt["cols"])
        rel = stmt["relation"]
        tbl = Table(
            rel["alias"]["aliasname"] if "alias" in rel.keys() else "", rel["relname"]
        )
        select = (
            self.__analyze_select(1, stmt["selectStmt"])
            if "selectStmt" in stmt.keys()
            else node.Select.empty()
        )
        return node.Insert(0, res, tbl, select)
