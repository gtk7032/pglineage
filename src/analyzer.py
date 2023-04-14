from typing import Any

from pglast import ast, parse_sql

import node
from column import Column
from restarget import ResTarget
from table import Table


class Analyzer:
    def __init__(self) -> None:
        self.__rawstmts: list[Any] = []

    def load(self, sqls: str) -> None:
        self.__rawstmts = [sql.stmt for sql in parse_sql(sqls)]

    def analyze(self) -> list[node.X]:
        nodes: list[node.X] = []
        for rawstmt in self.__rawstmts:
            if isinstance(rawstmt, ast.SelectStmt):
                nodes.append(self.__analyze_select(rawstmt(skip_none=True)))
            elif isinstance(rawstmt, ast.InsertStmt):
                nodes.append(self.__analyze_insert(rawstmt(skip_none=True)))
        return nodes

    def __analyze_fromclause(
        self, fc: dict[str, Any], tables: list[Table], layer: int = 0
    ) -> None:
        if "@" not in fc.keys():
            return

        if fc["@"] == "RangeSubselect":
            tables.append(
                Table(
                    fc["alias"]["aliasname"],
                    self.__analyze_select(fc["subquery"], layer + 1),
                )
            )

        elif fc["@"] == "RangeVar":
            tbl = Table(
                fc["alias"]["aliasname"] if "alias" in fc.keys() else "", fc["relname"]
            )
            if not [t for t in tables if str(t) == str(tbl)]:
                tables.append(tbl)

        for v in fc.values():
            if isinstance(v, dict):
                self.__analyze_fromclause(v, tables, layer)

    def __analyze_restargets(cls, tgtlist: list[dict[str, Any]]) -> list[ResTarget]:
        psdlst: list[ResTarget] = []

        for tgt in tgtlist:
            if "@" not in tgt.keys() or tgt["@"] != "ResTarget":
                Exception()

            name = tgt["name"] if "name" in tgt.keys() else ""
            refcols: list[Column] = []

            for v in tgt.values():
                if isinstance(v, tuple):
                    for vv in v:
                        cls.__extract_refcols(vv, refcols)
                else:
                    cls.__extract_refcols(v, refcols)

            psdlst.append(ResTarget(name, refcols))

        return psdlst

    def __extract_refcols(cls, tgt: dict[str, Any], refcols: list[Column]) -> None:
        if not isinstance(tgt, dict):
            return

        if "@" in tgt.keys() and tgt["@"] == "ColumnRef" and "fields" in tgt.keys():
            refcol = []
            for field in tgt["fields"]:
                if "sval" in field.keys():
                    refcol.append(field["sval"])
            if refcol:
                refcols.append(Column.create_from_list(refcol))
            return

        for v in tgt.values():
            if isinstance(v, tuple):
                for vv in v:
                    cls.__extract_refcols(vv, refcols)
            else:
                cls.__extract_refcols(v, refcols)

    def __analyze_select(
        self, statement: dict[str, Any], layer: int = 0
    ) -> node.Select:
        columns = self.__analyze_restargets(statement["targetList"])
        tables: list[Table] = []

        if "withClause" in statement.keys():
            for cte in statement["withClause"]["ctes"]:
                tables.append(
                    Table(
                        cte["ctename"],
                        self.__analyze_select(cte["ctequery"], layer + 1),
                    )
                )

        if "fromClause" in statement.keys():
            for fc in statement["fromClause"]:
                self.__analyze_fromclause(fc, tables, layer)

        if len(tables) == 1:
            for col in columns:
                col.attach_table(tables[0])

        return node.Select(columns, tables, layer)

    def __analyze_insert(self, stmt: dict[str, Any]) -> node.Insert:
        res = self.__analyze_restargets(stmt["cols"])
        rel = stmt["relation"]
        tbl = Table(
            rel["alias"]["aliasname"] if "alias" in rel.keys() else "", rel["relname"]
        )
        select = (
            self.__analyze_select(stmt["selectStmt"], 1)
            if "selectStmt" in stmt.keys()
            else node.Select.empty()
        )
        return node.Insert(res, tbl, select)
