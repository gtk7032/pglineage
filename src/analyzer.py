from typing import Any

from pglast import ast, parse_sql

import node
from column import Column
from table import Table


class Analyzer:
    def __init__(self) -> None:
        self.__rawstmts: list[Any] = []

    def load(self, sqls: str) -> None:
        self.__rawstmts.extend(sql.stmt for sql in parse_sql(sqls))

    def analyze(self) -> list[node.Node]:
        nodes: list[node.Node] = []
        for rawstmt in self.__rawstmts:
            if isinstance(rawstmt, ast.SelectStmt):
                nodes.append(self.__analyze_select(rawstmt(skip_none=True)))
            elif isinstance(rawstmt, ast.InsertStmt):
                nodes.append(self.__analyze_insert(rawstmt(skip_none=True)))
        return nodes

    def __analyze_fromclause(
        self, fc: dict[str, Any], tables: dict[str, Table], layer: int = 0
    ) -> None:
        if "@" not in fc.keys():
            return

        if fc["@"] == "RangeSubselect":
            tables[fc["alias"]["aliasname"]] = Table(
                self.__analyze_select(fc["subquery"], layer + 1),
            )

        elif fc["@"] == "RangeVar":
            tblnm = fc["alias"]["aliasname"] if "alias" in fc.keys() else fc["relname"]
            if tblnm not in tables.keys():
                tables[tblnm] = Table(fc["relname"])

        for v in fc.values():
            if isinstance(v, dict):
                self.__analyze_fromclause(v, tables, layer)

    def __analyze_restargets(
        self, restargets: list[dict[str, Any]]
    ) -> dict[str, list[Column]]:
        results: dict[str, list[Column]] = {}

        for i, tgt in enumerate(restargets):
            if "@" not in tgt.keys() or tgt["@"] != "ResTarget":
                Exception()

            refcols: list[Column] = []

            for v in tgt.values():
                if isinstance(v, tuple):
                    for vv in v:
                        self.__extract_refcols(vv, refcols)
                else:
                    self.__extract_refcols(v, refcols)

            name = (
                tgt["name"]
                if "name" in tgt.keys()
                else refcols[0].name
                if len(refcols) == 1
                else "column-" + str(i)
            )
            results[name] = refcols

        return results

    def __extract_refcols(self, tgt: dict[str, Any], refcols: list[Column]) -> None:
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
                    self.__extract_refcols(vv, refcols)
            else:
                self.__extract_refcols(v, refcols)

    def __analyze_select(
        self, statement: dict[str, Any], layer: int = 0
    ) -> node.Select:
        columns = self.__analyze_restargets(statement["targetList"])
        tables: dict[str, Table] = {}

        if "withClause" in statement.keys():
            for cte in statement["withClause"]["ctes"]:
                tables[cte["ctename"]] = Table(
                    self.__analyze_select(cte["ctequery"], layer + 1)
                )

        if "fromClause" in statement.keys():
            for fc in statement["fromClause"]:
                self.__analyze_fromclause(fc, tables, layer)

        if len(tables.keys()) == 1:
            for refcols in columns.values():
                for rc in refcols:
                    rc.set_table(next(iter(tables)))

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
